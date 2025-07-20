use papaya::{Compute, HashMap, Operation, OwnedGuard};
use std::sync::atomic::{AtomicIsize, Ordering};

use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

type PageId = u64;

pub struct Pager {
    map: HashMap<PageId, PageLock>,
}

struct PageLock {
    refcount: AtomicIsize,
    lock: RwLock<()>,
}

pub struct PageReadGuard<'a> {
    pager: &'a Pager,
    id: PageId,
    rw_guard: RwLockReadGuard<'a, ()>,
    map_guard: &'a OwnedGuard<'a>,
}

pub struct PageWriteGuard<'a> {
    pager: &'a Pager,
    id: PageId,
    rw_guard: RwLockWriteGuard<'a, ()>,
    map_guard: &'a OwnedGuard<'a>,
}

impl Drop for PageReadGuard<'_> {
    fn drop(&mut self) {
        try_gc(self.pager, self.id, self.map_guard);
    }
}

impl Drop for PageWriteGuard<'_> {
    fn drop(&mut self) {
        try_gc(self.pager, self.id, self.map_guard);
    }
}

fn try_gc(pager: &Pager, id: PageId, map_guard: &OwnedGuard) {
    let closure = |kv: Option<(&PageId, &PageLock)>| {
        match kv {
            Some((_id, v)) => {
                let PageLock {
                    refcount,
                    lock: _lock,
                } = v;

                // This can never go below zero, because there's only as many refcounts as threads holding a ref and we dont dec if it saturated.
                let refc = refcount.fetch_update(
                    // We use Release here so we don't reorder with compare exchange
                    Ordering::Release,
                    // We use relaxed here because we won't ever use this value.
                    Ordering::Relaxed,
                    |count| {
                        if count == isize::MAX {
                            None
                        } else {
                            Some(count - 1)
                        }
                    },
                );

                if refc == Ok(1) {
                    // We're last thread to have ref, so we'll try to GC this
                    let res = refcount.compare_exchange(
                        0,
                        isize::MIN,
                        // We use Ordering::Acquire here so that we don't reorder with the refcount decrement
                        // write of another thread (because that would cause a leak)
                        Ordering::Acquire,
                        // We use Relaxed because we don't actually use this value.
                        Ordering::Relaxed,
                    );
                    if res.is_ok() {
                        // We need to GC it because we've successfully swapped it.
                        return Operation::Remove;
                    }
                }
                // Nothing to do!
                Operation::Abort(())
            }
            None => {
                unreachable!(
                    "Because we hold the guard, it's not possible for the value to have been GC'd"
                );
            }
        }
    };
    pager.map.compute(id, closure, map_guard);
}

impl Pager {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub async fn read_page<'a>(
        &'a self,
        id: PageId,
        guard: &'a OwnedGuard<'a>,
    ) -> PageReadGuard<'a> {
        let lock = self.get_lock(id, guard);
        let rw_guard = lock.read().await;
        PageReadGuard {
            pager: self,
            id,
            rw_guard,
            map_guard: guard,
        }
    }

    pub async fn write_page<'a>(
        &'a self,
        id: PageId,
        guard: &'a OwnedGuard<'a>,
    ) -> PageWriteGuard<'a> {
        let lock = self.get_lock(id, guard);
        let rw_guard = lock.write().await;
        PageWriteGuard {
            pager: self,
            id,
            rw_guard,
            map_guard: guard,
        }
    }

    pub fn get_lock<'a>(&'a self, id: PageId, guard: &'a OwnedGuard<'a>) -> &'a RwLock<()> {
        fn closure<'a>(
            kv: Option<(&'a PageId, &'a PageLock)>,
        ) -> Operation<PageLock, &'a RwLock<()>> {
            match kv {
                // We're the first accessor, so we add a new entry.
                None => Operation::Insert(PageLock {
                    refcount: AtomicIsize::new(1),
                    lock: RwLock::new(()),
                }),
                Some((_id, PageLock { refcount, lock })) => {
                    // This can be done with relaxed ordering as we don't care if we read/write before or after a potential previous accesor's
                    // compare_exchange. If we do this before, then we keep the lock and all is well. If we do this after, then we merely insert
                    // a new lock and all is well.
                    let (Ok(refcount) | Err(refcount)) =
                        refcount.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                            // Should realistically never be hit in an actual program, but just in case.
                            // It's better to leak a few dozen bytes of memory than to potentially corrupt
                            // a database...
                            if count == isize::MAX {
                                None
                            } else {
                                Some(count + 1)
                            }
                        });
                    if refcount < 0 {
                        // Previous accessor is in process of removing. Either this insert will go through after the
                        // removal is over, or the operation will fail, this closure will rerun, and we will hit the None branch.
                        Operation::Insert(PageLock {
                            refcount: AtomicIsize::new(1),
                            lock: RwLock::new(()),
                        })
                    } else {
                        // Another thread still accessing this one, so we just increment the refcount.
                        Operation::Abort(lock)
                    }
                }
            }
        }
        let lock = self.map.compute(id, closure, guard);

        match lock {
            Compute::Inserted(_, PageLock { refcount: _, lock })
            | Compute::Updated {
                old: _,
                new: (_, PageLock { refcount: _, lock }),
            }
            | Compute::Aborted(lock) => lock,
            Compute::Removed(_, _) => {
                unreachable!("Previous code should have either inserted or incremented counter");
            }
        }
    }
}

use std::io::{Error, Read};

use byteorder::{LittleEndian, ReadBytesExt as _};

use crate::{
    pager::{PageId, Pager},
    types::{Type, Value},
};

struct BTree {
    start_page: PageId,
    pager: Pager,
}

enum PageType {
    InteriorIndex = 0,
    InteriorTable = 1,
    LeafIndex = 2,
    LeafTable = 3,
}

impl PageType {
    fn parse(n: u8) -> Result<Self, std::io::Error> {
        Ok(match n {
            0 => PageType::InteriorIndex,
            1 => PageType::InteriorTable,
            2 => PageType::LeafIndex,
            3 => PageType::LeafTable,
            _ => return Err(Error::other("Invalid table kind")),
        })
    }
}

const HEADER_SIZE: usize = 1 + 2 + 2 + 2 + 2 + 2;

struct PageHeader {
    // Page type (Interior index)
    page_type: PageType,
    // Pointer to start of section after header + pointer array
    freeblock_start: u16,
    // Number of cells in this page. Cell is (Pkey, PageId )
    cell_count: u16,
    cell_content_start: u16,
    total_free_bytes: u16,
}

impl PageHeader {
    fn parse(bytes: &[u8]) -> Result<Self, std::io::Error> {
        let mut slice = ReadableSlice::new(bytes);
        let page_type = slice.read_u8()?;
        let page_type = PageType::parse(page_type)?;
        let freeblock_start = slice.read_u16::<LittleEndian>()?;
        let cell_count = slice.read_u16::<LittleEndian>()?;
        let cell_content_start = slice.read_u16::<LittleEndian>()?;
        let total_free_bytes = slice.read_u16::<LittleEndian>()?;
        Ok(Self {
            page_type,
            freeblock_start,
            cell_count,
            cell_content_start,
            total_free_bytes,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ReadableSlice<'a>(&'a [u8], usize);

impl<'a> ReadableSlice<'a> {
    fn new(s: &'a [u8]) -> Self {
        Self(s, 0)
    }
}

impl<'a> Read for ReadableSlice<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.1 >= self.0.len() {
            return Ok(0);
        }
        let available = self.0.len() - self.1;
        let buf_len = buf.len();
        let amt = available.min(buf_len);
        buf[..amt].copy_from_slice(&self.0[..amt]);
        self.1 += amt;
        Ok(amt)
    }
}

impl BTree {
    fn find(&self, key: &[u8]) -> Result<Option<(PageId, u16)>, std::io::Error> {
        let mut page_id = self.start_page;
        let guard = self.pager.get_guard();
        let mut buf = vec![0; self.pager.page_size as usize];
        let mut file = self.pager.open_file()?;
        loop {
            let read = self.pager.read_page(page_id, &guard);
            read.read_into(&mut buf, &mut file)?;
            let header = PageHeader::parse(&buf)?;
        }
    }
}

trait BtreeKey {
    fn write_to_buf(&self, buf: &mut Vec<u8>);
    fn read_from_buf(buf: &[u8], typ: Type) -> (Value, usize);
}

impl BtreeKey for Value<'_> {
    fn write_to_buf(&self, buf: &mut Vec<u8>) {
        match self {
            Value::OwnedBytes(bytes) => Value::Bytes(bytes).write_to_buf(buf),
            Value::OwnedString(s) => Value::String(s).write_to_buf(buf),
            Value::Bytes(bytes) => {
                let len = (bytes.len() as u64).to_le_bytes();
                buf.copy_from_slice(&len);
                buf.copy_from_slice(bytes);
            }
            Value::String(string) => {
                let len = (string.len() as u64).to_le_bytes();
                buf.copy_from_slice(&len);
                buf.copy_from_slice(string.as_bytes());
            }
            Value::OverflowedBytes(_) => todo!(),
            Value::OverflowedString(_) => todo!(),
            Value::U64(n) => buf.copy_from_slice(&n.to_le_bytes()),
            Value::U32(n) => buf.copy_from_slice(&n.to_le_bytes()),
            Value::I64(n) => buf.copy_from_slice(&n.to_le_bytes()),
            Value::I32(n) => buf.copy_from_slice(&n.to_le_bytes()),
            Value::F32(n) => buf.copy_from_slice(&n.to_le_bytes()),
            Value::F64(n) => buf.copy_from_slice(&n.to_le_bytes()),
        }
    }

    fn read_from_buf(buf: &[u8], typ: Type) -> (Value, usize) {
        match typ {
            Type::Blob => {
                let len = u64::from_le_bytes(buf[..8].try_into().unwrap()) as usize;
                let blob = &buf[8..8 + len];
                (Value::Bytes(blob), len + 8)
            }
            Type::String => {
                let len = u64::from_le_bytes(buf[..8].try_into().unwrap()) as usize;
                let blob = &buf[8..8 + len];
                (Value::String(str::from_utf8(blob).unwrap()), len + 8)
            }
            Type::U64 => (
                Value::U64(u64::from_le_bytes(buf[..8].try_into().unwrap())),
                8,
            ),
            Type::U32 => (
                Value::U32(u32::from_le_bytes(buf[..4].try_into().unwrap())),
                4,
            ),
            Type::I64 => (
                Value::I64(i64::from_le_bytes(buf[..8].try_into().unwrap())),
                8,
            ),
            Type::I32 => (
                Value::I32(i32::from_le_bytes(buf[..4].try_into().unwrap())),
                4,
            ),
            Type::F64 => (
                Value::F64(f64::from_le_bytes(buf[..8].try_into().unwrap())),
                8,
            ),
            Type::F32 => (
                Value::F32(f32::from_le_bytes(buf[..4].try_into().unwrap())),
                4,
            ),
        }
    }
}

pub enum Type {
    Blob,
    String,
    U64,
    U32,
    I64,
    I32,
    F32,
    F64,
}
pub enum Value<'a> {
    OwnedBytes(Vec<u8>),
    OwnedString(String),
    Bytes(&'a [u8]),
    String(&'a str),
    OverflowedBytes(u64),
    OverflowedString(u64),
    U64(u64),
    U32(u32),
    I64(i64),
    I32(i32),
    F32(f32),
    F64(f64),
}

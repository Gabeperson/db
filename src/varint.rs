#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct varint(pub u64);

impl varint {
    #[must_use]
    pub fn encode_varint(self) -> ([u8; 10], usize) {
        let mut value = self.0;
        let mut index = 0;
        let mut buf = [0u8; 10];
        while value >= 0x80 {
            buf[index] = (value as u8 & 0x7F) | 0x80;
            index += 1;
            value >>= 7;
        }
        buf[index] = value as u8;
        index += 1;
        (buf, index)
    }

    #[must_use]
    pub fn decode_varint(buf: &[u8]) -> Option<(u64, usize)> {
        let mut result = 0;
        for i in 0..10 {
            let byte = *buf.get(i)?;
            result |= u64::from(byte & 0x7f) << (7 * i);
            if byte & 0x80 == 0 {
                return Some((result, i + 1));
            }
        }
        None
    }
}

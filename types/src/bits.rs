use crate::{CompareEq, HustleType};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Bits {
    len: usize,
    byte_len: usize,
}

impl Bits {
    pub fn new(len: usize) -> Self {
        assert!(len > 0, "Cannot create Bits type with zero length");
        let byte_len = 1 + ((len - 1) / 8); // Ceiling of len / 8.
        Bits {
            len,
            byte_len,
        }
    }

    pub fn get(&self, i: usize, buf: &[u8]) -> bool {
        let (byte_i, mask) = self.mask(i);
        buf[byte_i] & mask != 0
    }

    pub fn set(&self, i: usize, val: bool, buf: &mut [u8]) {
        let (byte_i, mask) = self.mask(i);
        if val {
            buf[byte_i] |= mask;
        } else {
            buf[byte_i] &= !mask;
        }
    }

    fn mask(&self, i: usize) -> (usize, u8) {
        assert!(i < self.len, "Bit {} out of range for length {}", i, self.len);
        (i / 8, (1 << i % 8) as u8)
    }
}

impl HustleType for Bits {
    fn byte_len(&self) -> usize {
        self.byte_len
    }

    fn to_string(&self, buf: &[u8]) -> String {
        let mut s = String::new();
        for bit_i in 0..self.len {
            let b = if self.get(bit_i, buf) { '1' } else { '0' };
            s.push(b);
        }
        s
    }
}

impl CompareEq<Bits> for Bits {
    fn compare_eq(&self, other: &Bits, left: &[u8], right: &[u8]) -> bool {
        self.len == other.len && left == right
    }
}

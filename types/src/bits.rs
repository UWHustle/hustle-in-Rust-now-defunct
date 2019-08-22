use crate::HustleType;

#[derive(Serialize, Deserialize)]
pub struct Bits {
    len: usize,
    byte_len: usize,
}

impl Bits {
    pub fn new(len: usize) -> Self {
        let byte_len = 1 + ((len - 1) / 8); // Ceiling of len / 8.
        Bits {
            len,
            byte_len,
        }
    }

    pub fn get(&self, i: usize, buf: &[u8]) -> bool {
        let (byte_i, mask) = Self::mask(i);
        buf[byte_i] & mask != 0
    }

    pub fn set(&self, i: usize, val: bool, buf: &mut [u8]) {
        let (byte_i, mask) = Self::mask(i);
        if val {
            buf[byte_i] |= mask;
        } else {
            buf[byte_i] &= !mask;
        }
    }

    fn mask(i: usize) -> (usize, u8) {
        (i / 8, (1 << i % 8) as u8)
    }
}

impl HustleType for Bits {
    fn byte_len(&self) -> usize {
        self.byte_len
    }
}

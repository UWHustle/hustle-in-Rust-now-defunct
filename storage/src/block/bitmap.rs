use block::RawSlice;

#[derive(Clone)]
pub struct BitMap {
    blocks: RawSlice,
}

impl BitMap {
    pub fn new(buf: &mut [u8]) -> Self {
        BitMap {
            blocks: RawSlice::new(buf),
        }
    }

    pub fn get_unchecked(&self, i: usize) -> bool {
        let (block_i, mask) = Self::mask(i);
        self.blocks.as_slice()[block_i] & mask != 0
    }

    pub fn set_unchecked(&mut self, i: usize, value: bool) {
        let (block_i, mask) = Self::mask(i);
        if value {
            self.blocks.as_slice()[block_i] |= mask;
        } else {
            self.blocks.as_slice()[block_i] &= !mask;
        }
    }

    pub fn set_all(&mut self, value: bool) {
        let v = if value { u8::max_value() } else { u8::min_value() };
        for block in self.blocks.as_slice().iter_mut() {
            *block = v
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = bool> + '_ {
        self.blocks.as_slice().iter()
            .flat_map(|block|
                (0..8).map(move |shift| block & (1 << shift) != 0)
            )
    }

    fn mask(i: usize) -> (usize, u8) {
        let block_i = i / 8;
        let bit_i = i % 8;
        let mask = 1 << bit_i as u8;
        (block_i, mask)
    }
}

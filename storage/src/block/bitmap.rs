use std::ops::DerefMut;

#[derive(Clone)]
pub struct BitMap<D> {
    blocks: D,
    len: usize,
}

impl<D> BitMap<D> where D: DerefMut<Target = [u8]> {
    pub fn new(blocks: D) -> Self {
        let len = 8 * blocks.len();
        BitMap {
            blocks,
            len,
        }
    }

    pub fn get_unchecked(&self, i: usize) -> bool {
        let (block_i, mask) = Self::mask(i);
        self.blocks[block_i] & mask != 0
    }

    pub fn set_unchecked(&mut self, i: usize, value: bool) {
        let (block_i, mask) = Self::mask(i);
        if value {
            self.blocks[block_i] |= mask;
        } else {
            self.blocks[block_i] &= !mask;
        }
    }

    pub fn set_all(&mut self, value: bool) {
        let v = if value { u8::max_value() } else { u8::min_value() };
        for block in self.blocks.iter_mut() {
            *block = v
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = bool> + '_ {
        self.blocks.iter()
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

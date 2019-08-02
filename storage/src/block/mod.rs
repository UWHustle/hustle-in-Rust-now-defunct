pub use self::header::Header;
pub use self::bitmap::{BitMap};
pub use self::row_major::RowMajorBlock;
pub use self::reference::{BlockReference};
use std::ops::{Deref, DerefMut};
use std::slice;

mod row_major;
mod header;
mod bitmap;
mod reference;

pub const BLOCK_SIZE: usize = 4096;

#[derive(Clone)]
struct RawSlice {
    data: *mut u8,
    len: usize,
}

impl RawSlice {
    fn new(s: &mut [u8]) -> Self {
        RawSlice {
            data: s.as_mut_ptr(),
            len: s.len(),
        }
    }
}

impl Deref for RawSlice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { slice::from_raw_parts(self.data, self.len) }
    }
}

impl DerefMut for RawSlice {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { slice::from_raw_parts_mut(self.data, self.len) }
    }
}

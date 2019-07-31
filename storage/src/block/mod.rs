pub use self::header::Header;
pub use self::bitmap::{BitMap};
pub use self::row_major::{RowMajorBlock, RowBuilder};

mod row_major;
mod header;
mod bitmap;

const BLOCK_SIZE: usize = 4096;

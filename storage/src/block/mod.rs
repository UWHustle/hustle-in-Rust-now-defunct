pub use self::header::Header;
pub use self::bitmap::{BitMap};
pub use self::relational_block::{RelationalBlock, RowBuilder};

mod relational_block;
mod header;
mod bitmap;

const BLOCK_SIZE: usize = 4096;

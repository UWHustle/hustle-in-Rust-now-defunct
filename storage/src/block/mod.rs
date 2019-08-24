pub use self::column_major::ColumnMajorBlock;
pub use self::reference::BlockReference;
pub use self::latch::Latch;

mod reference;
mod column_major;
mod latch;

pub const BLOCK_SIZE: usize = 4096;

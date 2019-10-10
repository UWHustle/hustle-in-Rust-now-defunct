pub use self::column_major::{ColumnMajorBlock, RowMask};
pub use self::reference::BlockReference;

mod reference;
mod column_major;

pub const BLOCK_SIZE: usize = 2^20;

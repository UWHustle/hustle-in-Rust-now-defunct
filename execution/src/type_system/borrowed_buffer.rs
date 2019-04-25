use super::*;

/// Value is stored in a buffer with another owner - we just have a reference
#[derive(PartialEq, Eq, Hash)]
pub struct BorrowedBuffer<'a> {
    data: &'a [u8],
    data_type: DataType,
    is_null: bool,
}

impl<'a> BorrowedBuffer<'a> {
    pub fn new(data: &'a [u8], data_type: DataType, is_null: bool) -> Self {
        BorrowedBuffer {
            data_type,
            is_null,
            data,
        }
    }
}

impl<'a> Buffer for BorrowedBuffer<'a> {
    fn data(&self) -> &[u8] {
        &self.data
    }

    fn data_type(&self) -> DataType {
        self.data_type.clone()
    }

    fn is_null(&self) -> bool {
        self.is_null
    }
}

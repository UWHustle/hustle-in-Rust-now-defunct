use super::*;

/// Value is stored in a buffer with another owner - we just have a reference
pub struct BorrowedBuffer<'a> {
    type_id: TypeID,
    is_null: bool,
    data: &'a [u8],
}

impl<'a> BorrowedBuffer<'a> {
    pub fn new(type_id: TypeID, is_null: bool, data: &'a [u8]) -> Self {
        BorrowedBuffer {
            type_id,
            is_null,
            data,
        }
    }
}

impl<'a> Buffer for BorrowedBuffer<'a> {
    fn type_id(&self) -> TypeID {
        self.type_id.clone()
    }

    fn is_null(&self) -> bool {
        self.is_null
    }

    fn data(&self) -> &[u8] {
        &self.data
    }
}
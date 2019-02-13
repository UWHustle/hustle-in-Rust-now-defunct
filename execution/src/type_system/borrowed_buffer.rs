use super::*;

/// Value is stored in a buffer with another owner - we just have a reference
pub struct BorrowedBuffer<'a> {
    data: &'a [u8],
    type_id: TypeID,
    is_null: bool,
}

impl<'a> BorrowedBuffer<'a> {
    pub fn new(data: &'a [u8], type_id: TypeID, is_null: bool) -> Self {
        BorrowedBuffer {
            type_id,
            is_null,
            data,
        }
    }
}

impl<'a> Buffer for BorrowedBuffer<'a> {
    fn data(&self) -> &[u8] {
        &self.data
    }

    fn type_id(&self) -> TypeID {
        self.type_id.clone()
    }

    fn is_null(&self) -> bool {
        self.is_null
    }
}
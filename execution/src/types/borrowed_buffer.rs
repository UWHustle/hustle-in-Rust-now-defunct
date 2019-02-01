use super::TypeID;
use super::BufferType;

// Value is stored in a buffer with another owner - we just have a reference
// The lifetime specifier ensures the data's lifetime exceeds this struct's
struct BorrowedBuffer<'a> {
    type_id: TypeID,
    data: &'a [u8],
}

impl<'a> BorrowedBuffer<'a> {
    fn new(type_id: TypeID, data: &'a [u8]) -> Self {
        BorrowedBuffer {
            type_id,
            data,
        }
    }
}

impl<'a> BufferType for BorrowedBuffer<'a> {
    fn type_id(&self) -> TypeID {
        self.type_id.clone()
    }

    fn data(&self) -> &[u8] {
        &self.data
    }
}
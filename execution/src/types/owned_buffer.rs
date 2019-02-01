use super::BufferType;
use super::TypeID;

pub struct OwnedBuffer {
    type_id: TypeID,
    data: Vec<u8>,
}

impl OwnedBuffer {
    pub fn new(type_id: TypeID, data: Vec<u8>) -> Self {
        OwnedBuffer {
            type_id,
            data,
        }
    }
}

impl BufferType for OwnedBuffer {
    fn type_id(&self) -> &TypeID {
        &self.type_id
    }
    fn data(&self) -> &[u8] {
        &self.data
    }
}
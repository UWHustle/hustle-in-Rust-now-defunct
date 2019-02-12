use super::BufferType;
use super::TypeID;

pub struct OwnedBuffer {
    type_id: TypeID,
    is_null: bool,
    data: Vec<u8>,
}

impl OwnedBuffer {
    pub fn new(type_id: TypeID, is_null: bool, data: Vec<u8>) -> Self {
        OwnedBuffer {
            type_id,
            is_null,
            data,
        }
    }
}

impl BufferType for OwnedBuffer {
    fn type_id(&self) -> TypeID {
        self.type_id.clone()
    }

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn is_null(&self) -> bool {
        self.is_null
    }
}
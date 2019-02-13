use super::*;

/// Value is stored in a `Vec<u8>` owned by this struct
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

impl Buffer for OwnedBuffer {
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
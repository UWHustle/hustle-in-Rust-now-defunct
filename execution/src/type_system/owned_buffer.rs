use super::*;

/// Value is stored in a `Vec<u8>` owned by this struct
pub struct OwnedBuffer {
    data: Vec<u8>,
    type_id: TypeID,
    is_null: bool,
}

impl OwnedBuffer {
    pub fn new(data: Vec<u8>, type_id: TypeID, is_null: bool) -> Self {
        OwnedBuffer {
            data,
            type_id,
            is_null,
        }
    }
}

impl Buffer for OwnedBuffer {
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

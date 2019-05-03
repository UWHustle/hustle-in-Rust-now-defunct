use super::*;

/// Value is stored in a `Vec<u8>` owned by this struct
pub struct OwnedBuffer {
    data: Vec<u8>,
    data_type: DataType,
    is_null: bool,
}

impl OwnedBuffer {
    pub fn new(data: Vec<u8>, data_type: DataType, is_null: bool) -> Self {
        OwnedBuffer {
            data,
            data_type,
            is_null,
        }
    }
}

impl Buffer for OwnedBuffer {
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

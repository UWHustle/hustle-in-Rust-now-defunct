extern crate byteorder;

use self::byteorder::{ByteOrder, LittleEndian};

use super::cast;
use super::integer::*;
use super::ValueType;
use super::owned_buffer::OwnedBuffer;
use super::TypeID;

pub struct UTF8String {
    value: Box<String>
}

impl UTF8String {
    pub fn new(data: &[u8]) -> Self {
        let mut vec_data: Vec<u8> = vec!();
        vec_data.clone_from_slice(data);
        let value = String::from_utf8(vec_data).expect("Invalid UTF8 string");
        UTF8String { value: Box::new(value) }
    }
    pub fn value(&self) -> &str {
        &self.value
    }
}

impl ValueType for UTF8String {
    fn un_marshall(&self) -> OwnedBuffer {
        let mut value: Vec<u8> = vec!();
        value.clone_from_slice(self.value.as_bytes());
        OwnedBuffer::new(self.type_id(), value)
    }
    fn equals(&self, other: &ValueType) -> bool {
        match other.type_id() {
            _ => panic!(),
        }
    }
    fn type_id(&self) -> TypeID {
        TypeID::UTF8String
    }
}
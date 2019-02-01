extern crate byteorder;

use self::byteorder::{ByteOrder, LittleEndian};

use super::owned_buffer::OwnedBuffer;
use super::ValueType;
use super::TypeID;

// TODO: Define behavior an IP address should have
trait IPAddress: ValueType {}

pub struct IPv4 {
    value: u32
}

impl IPv4 {
    pub fn new(data: &[u8]) -> Self {
        IPv4 { value: LittleEndian::read_u32(&data) }
    }
    pub fn value(&self) -> u32 {
        self.value
    }
}

impl ValueType for IPv4 {
    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; 4];
        LittleEndian::write_u32(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), data)
    }
    fn equals(&self, other: &ValueType) -> bool {
        match other.type_id() {
            _ => panic!(),
        }
    }
    fn type_id(&self) -> TypeID {
        TypeID::Float8
    }
}
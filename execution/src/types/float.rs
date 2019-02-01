extern crate byteorder;

use self::byteorder::{ByteOrder, LittleEndian};

use super::cast;
use super::integer::*;
use super::ValueType;
use super::owned_buffer::OwnedBuffer;
use super::TypeID;

// Define common methods on floating-point types here
trait Float: ValueType {}

pub struct Float8 {
    value: f64
}

impl Float8 {
    pub fn new(data: &[u8]) -> Self {
        Float8 { value: LittleEndian::read_f64(&data) }
    }
    pub fn value(&self) -> f64 {
        self.value
    }
}

impl ValueType for Float8 {
    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; 8];
        LittleEndian::write_f64(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), data)
    }
    fn equals(&self, other: &ValueType) -> bool {
        match other.type_id() {
            TypeID::Int2 => self.value == cast::<Int2>(other).value() as f64,
            TypeID::Float8 => self.value == cast::<Float8>(other).value,
            _ => panic!(),
        }
    }
    fn type_id(&self) -> TypeID {
        TypeID::Float8
    }
}
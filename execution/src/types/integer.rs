extern crate byteorder;

use self::byteorder::{ByteOrder, LittleEndian};

use super::cast;
use super::float::Float8;
use super::ValueType;
use super::owned_buffer::OwnedBuffer;
use super::TypeID;

// TODO: Define behavior an integer should have
trait Integer: ValueType {}

pub struct Int2 {
    value: i16
}

impl Int2 {
    pub fn new(data: &[u8]) -> Self {
        Int2 { value: LittleEndian::read_i16(&data) }
    }
    pub fn value(&self) -> i16 {
        self.value
    }
}

impl ValueType for Int2 {
    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; 2];
        LittleEndian::write_i16(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), data)
    }
    fn equals(&self, other: &ValueType) -> bool {
        match other.type_id() {
            TypeID::Int2 => self.value == cast::<Int2>(other).value,
            TypeID::Float8 => self.value as f64 == cast::<Float8>(other).value(),
            _ => panic!(),
        }
    }
    fn type_id(&self) -> TypeID {
        TypeID::Int2
    }
}

pub struct Int4 {
    value: i32
}

impl Int4 {
    pub fn new(data: &[u8]) -> Self {
        Int4 { value: LittleEndian::read_i32(&data) }
    }
    pub fn value(&self) -> i32 {
        self.value
    }
}

impl ValueType for Int4 {
    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; 4];
        LittleEndian::write_i32(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), data)
    }
    fn equals(&self, other: &ValueType) -> bool {
        match other.type_id() {
            TypeID::Int4 => self.value == cast::<Int4>(other).value(),
            TypeID::Float8 => self.value as f64 == cast::<Float8>(other).value(),
            _ => panic!(),
        }
    }
    fn type_id(&self) -> TypeID {
        TypeID::Int2
    }
}

pub struct Int8 {
    value: i64
}

impl Int8 {
    pub fn new(data: &[u8]) -> Self {
        Int8 { value: LittleEndian::read_i64(&data) }
    }
    pub fn value(&self) -> i64 {
        self.value
    }
}

impl ValueType for Int8 {
    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; 8];
        LittleEndian::write_i64(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), data)
    }
    fn equals(&self, other: &ValueType) -> bool {
        match other.type_id() {
            TypeID::Int8 => self.value == cast::<Int8>(other).value,
            TypeID::Float8 => self.value as f64 == cast::<Float8>(other).value(),
            _ => panic!(),
        }
    }
    fn type_id(&self) -> TypeID {
        TypeID::Int2
    }
}

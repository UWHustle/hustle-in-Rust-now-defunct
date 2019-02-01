extern crate byteorder;

use self::byteorder::{ByteOrder, LittleEndian};

use super::*;
use super::integer::*;
use super::ip_address::*;
use super::owned_buffer::OwnedBuffer;

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

    // Helper for getting a u8 from a comparison
    fn cmp(&self, other: f64) -> u8 {
        if self.value < other {
            -1
        } else if self.value > other {
            1
        } else {
            0
        }
    }
}

impl ValueType for Float8 {
    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; self.size()];
        LittleEndian::write_f64(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), data)
    }

    fn size(&self) -> usize { 8 }

    fn equals(&self, other: &ValueType) -> bool {
        match other.type_id() {
            TypeID::Int2 => {
                self.value == cast::<Int2>(other).value() as f64
            }
            TypeID::Int4 => {
                self.value == cast::<Int4>(other).value() as f64
            }
            TypeID::Int8 => {
                self.value == cast::<Int8>(other).value() as f64
            }
            TypeID::Float8 => {
                self.value == cast::<Float8>(other).value
            }
            TypeID::IPv4 => {
                self.value == cast::<IPv4>(other).value() as f64
            }
            _ => {
                panic!(incompatible_types(self.type_id(), other.type_id()));
            }
        }
    }

    fn compare(&self, other: &ValueType) -> u8 {
        match other.type_id() {
            TypeID::Int2 => {
                self.cmp(cast::<Int2>(other).value() as f64)
            }
            TypeID::Int4 => {
                self.cmp(cast::<Int4>(other).value() as f64)
            }
            TypeID::Int8 => {
                self.cmp(cast::<Int8>(other).value() as f64)
            }
            TypeID::Float8 => {
                self.cmp(cast::<Float8>(other).value)
            }
            TypeID::IPv4 => {
                self.cmp(cast::<IPv4>(other).value() as f64)
            }
            _ => {
                panic!(incompatible_types(self.type_id(), other.type_id()))
            }
        }
    }

    fn type_id(&self) -> TypeID { TypeID::Float8 }
}

#[cfg(test)]
mod test {
    // TODO: Place unit tests here
}
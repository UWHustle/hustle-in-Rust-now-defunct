extern crate byteorder;

use self::byteorder::{ByteOrder, LittleEndian};

use super::*;

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

impl Float for Float8 {}

impl ValueType for Float8 {
    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; self.size()];
        LittleEndian::write_f64(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), data)
    }

    fn size(&self) -> usize { 8 }

    fn type_id(&self) -> TypeID {
        TypeID::Float8
    }

    fn equals(&self, other: &ValueType) -> bool {
        match other.type_id() {
            TypeID::Int2 => {
                self.value.eq(&(cast::<Int2>(other).value() as f64))
            }
            TypeID::Int4 => {
                self.value.eq(&(cast::<Int4>(other).value() as f64))
            }
            TypeID::Int8 => {
                self.value.eq(&(cast::<Int8>(other).value() as f64))
            }
            TypeID::Float8 => {
                self.value.eq(&(cast::<Float8>(other).value()))
            }
            TypeID::IPv4 => {
                self.value.eq(&(cast::<IPv4>(other).value() as f64))
            }
            _ => false
        }
    }

    fn less_than(&self, other: &ValueType) -> bool {
        match other.type_id() {
            TypeID::Int2 => {
                self.value.lt(&(cast::<Int2>(other).value() as f64))
            }
            TypeID::Int4 => {
                self.value.lt(&(cast::<Int4>(other).value() as f64))
            }
            TypeID::Int8 => {
                self.value.lt(&(cast::<Int8>(other).value() as f64))
            }
            TypeID::Float8 => {
                self.value.lt(&(cast::<Float8>(other).value()))
            }
            TypeID::IPv4 => {
                self.value.lt(&(cast::<IPv4>(other).value() as f64))
            }
            _ => false
        }
    }

    fn greater_than(&self, other: &ValueType) -> bool {
        other.less_than(self)
    }
}

#[cfg(test)]
mod test {
    // TODO: Place unit tests here
}
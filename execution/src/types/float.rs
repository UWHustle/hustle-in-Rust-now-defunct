extern crate byteorder;

use self::byteorder::{ByteOrder, LittleEndian};

use super::*;

// Define common methods on floating-point types here
trait Float: ValueType {}

pub struct Float4 {
    value: f32
}

impl Float4 {
    pub fn new(value: f32) -> Self {
        Float4 { value }
    }

    pub fn marshall(data: &[u8]) -> Self {
        Float4 { value: LittleEndian::read_f32(&data) }
    }

    pub fn value(&self) -> f32 {
        self.value
    }
}

impl Float for Float4 {}

impl ValueType for Float4 {
    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; self.size()];
        LittleEndian::write_f32(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), data)
    }

    fn size(&self) -> usize { 4 }

    fn type_id(&self) -> TypeID {
        TypeID::Float4
    }

    fn compare(&self, other: &ValueType, comp: Comparator) -> bool {
        match other.type_id() {
            TypeID::Int2 => {
                comp.apply(self.value, cast::<Int2>(other).value() as f32)
            }
            TypeID::Int4 => {
                comp.apply(self.value, cast::<Int4>(other).value() as f32)
            }
            TypeID::Int8 => {
                comp.apply(self.value as f64, cast::<Int8>(other).value() as f64)
            }
            TypeID::Float4 => {
                comp.apply(self.value, cast::<Float4>(other).value())
            }
            TypeID::Float8 => {
                comp.apply(self.value as f64, cast::<Float8>(other).value())
            }
            TypeID::IPv4 => {
                comp.apply(self.value, cast::<IPv4>(other).value() as f32)
            }
            _ => false
        }
    }
}


pub struct Float8 {
    value: f64
}

impl Float8 {
    pub fn new(value: f64) -> Self {
        Float8 { value }
    }

    pub fn marshall(data: &[u8]) -> Self {
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

    fn compare(&self, other: &ValueType, comp: Comparator) -> bool {
        match other.type_id() {
            TypeID::Int2 => {
                comp.apply(self.value, cast::<Int2>(other).value() as f64)
            }
            TypeID::Int4 => {
                comp.apply(self.value, cast::<Int4>(other).value() as f64)
            }
            TypeID::Int8 => {
                comp.apply(self.value, cast::<Int8>(other).value() as f64)
            }
            TypeID::Float4 => {
                comp.apply(self.value, cast::<Float4>(other).value() as f64)
            }
            TypeID::Float8 => {
                comp.apply(self.value, cast::<Float8>(other).value())
            }
            TypeID::IPv4 => {
                comp.apply(self.value, cast::<IPv4>(other).value() as f64)
            }
            _ => false
        }
    }
}

#[cfg(test)]
mod test {
    // TODO: Place unit tests here
}
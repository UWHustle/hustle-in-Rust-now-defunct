extern crate byteorder;

use self::byteorder::{ByteOrder, LittleEndian};

use super::*;

// Define common methods on integer types here
pub trait Integer: ValueType {}

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

impl Integer for Int2 {}

impl ValueType for Int2 {
    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; self.size()];
        LittleEndian::write_i16(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), data)
    }

    fn size(&self) -> usize { 2 }

    fn type_id(&self) -> TypeID {
        TypeID::Int2
    }

    fn compare(&self, other: &ValueType, comp: Comparator) -> bool {
        match other.type_id() {
            TypeID::Int2 => {
                apply_comp(self.value, cast::<Int2>(other).value(), comp)
            }
            TypeID::Int4 => {
                apply_comp(self.value as i32, cast::<Int4>(other).value(), comp)
            }
            TypeID::Int8 => {
                apply_comp(self.value as i64, cast::<Int8>(other).value(), comp)
            }
            TypeID::Float4 => {
                apply_comp(self.value as f32, cast::<Float4>(other).value(), comp)
            }
            TypeID::Float8 => {
                apply_comp(self.value as f64, cast::<Float8>(other).value(), comp)
            }
            TypeID::IPv4 => {
                apply_comp(self.value as i64, cast::<IPv4>(other).value() as i64, comp)
            }
            _ => false
        }
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

impl Integer for Int4 {}

impl ValueType for Int4 {
    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; self.size()];
        LittleEndian::write_i32(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), data)
    }

    fn size(&self) -> usize { 4 }

    fn type_id(&self) -> TypeID {
        TypeID::Int4
    }

    fn compare(&self, other: &ValueType, comp: Comparator) -> bool {
        match other.type_id() {
            TypeID::Int2 => {
                apply_comp(self.value, cast::<Int2>(other).value() as i32, comp)
            }
            TypeID::Int4 => {
                apply_comp(self.value, cast::<Int4>(other).value(), comp)
            }
            TypeID::Int8 => {
                apply_comp(self.value as i64, cast::<Int8>(other).value(), comp)
            }
            TypeID::Float4 => {
                apply_comp(self.value as f32, cast::<Float4>(other).value(), comp)
            }
            TypeID::Float8 => {
                apply_comp(self.value as f64, cast::<Float8>(other).value(), comp)
            }
            TypeID::IPv4 => {
                apply_comp(self.value as i64, cast::<IPv4>(other).value() as i64, comp)
            }
            _ => false
        }
    }
}

pub struct Int8 {
    value: i64
}

impl Integer for Int8 {}

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
        let mut data: Vec<u8> = vec![0; self.size()];
        LittleEndian::write_i64(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), data)
    }

    fn size(&self) -> usize { 8 }

    fn type_id(&self) -> TypeID {
        TypeID::Int8
    }

    fn compare(&self, other: &ValueType, comp: Comparator) -> bool {
        match other.type_id() {
            TypeID::Int2 => {
                apply_comp(self.value, cast::<Int2>(other).value() as i64, comp)
            }
            TypeID::Int4 => {
                apply_comp(self.value, cast::<Int4>(other).value() as i64, comp)
            }
            TypeID::Int8 => {
                apply_comp(self.value, cast::<Int8>(other).value(), comp)
            }
            TypeID::Float4 => {
                apply_comp(self.value as f64, cast::<Float4>(other).value() as f64, comp)
            }
            TypeID::Float8 => {
                apply_comp(self.value as f64, cast::<Float8>(other).value(), comp)
            }
            TypeID::IPv4 => {
                apply_comp(self.value, cast::<IPv4>(other).value() as i64, comp)
            }
            _ => false
        }
    }
}

#[cfg(test)]
mod test {
    // TODO: Place unit tests here
}

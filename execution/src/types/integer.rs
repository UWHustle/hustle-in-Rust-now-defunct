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

    fn equals(&self, other: &ValueType) -> bool {
        match other.type_id() {
            TypeID::Int2 => {
                self.value.eq(&(cast::<Int2>(other).value()))
            }
            TypeID::Int4 => {
                (self.value as i32).eq(&(cast::<Int4>(other).value()))
            }
            TypeID::Int8 => {
                (self.value as i64).eq(&(cast::<Int8>(other).value()))
            }
            TypeID::Float8 => {
                (self.value as f64).eq(&(cast::<Float8>(other).value()))
            }
            TypeID::IPv4 => {
                (self.value as i64).eq(&(cast::<IPv4>(other).value() as i64))
            }
            _ => false
        }
    }

    fn less_than(&self, other: &ValueType) -> bool {
        match other.type_id() {
            TypeID::Int2 => {
                self.value.lt(&(cast::<Int2>(other).value()))
            }
            TypeID::Int4 => {
                (self.value as i32).lt(&(cast::<Int4>(other).value()))
            }
            TypeID::Int8 => {
                (self.value as i64).lt(&(cast::<Int8>(other).value()))
            }
            TypeID::Float8 => {
                (self.value as f64).lt(&(cast::<Float8>(other).value()))
            }
            TypeID::IPv4 => {
                (self.value as i64).lt(&(cast::<IPv4>(other).value() as i64))
            }
            _ => false
        }
    }

    fn greater_than(&self, other: &ValueType) -> bool {
        match other.type_id() {
            TypeID::Int2 => {
                self.value.gt(&(cast::<Int2>(other).value()))
            }
            TypeID::Int4 => {
                (self.value as i32).gt(&(cast::<Int4>(other).value()))
            }
            TypeID::Int8 => {
                (self.value as i64).gt(&(cast::<Int8>(other).value()))
            }
            TypeID::Float8 => {
                (self.value as f64).gt(&(cast::<Float8>(other).value()))
            }
            TypeID::IPv4 => {
                (self.value as i64).gt(&(cast::<IPv4>(other).value() as i64))
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

    fn equals(&self, other: &ValueType) -> bool {
        match other.type_id() {
            TypeID::Int2 => {
                self.value.eq(&(cast::<Int2>(other).value() as i32))
            }
            TypeID::Int4 => {
                self.value.eq(&(cast::<Int4>(other).value()))
            }
            TypeID::Int8 => {
                (self.value as i64).eq(&(cast::<Int8>(other).value()))
            }
            TypeID::Float8 => {
                (self.value as f64).eq(&(cast::<Float8>(other).value()))
            }
            TypeID::IPv4 => {
                (self.value as i64).eq(&(cast::<IPv4>(other).value() as i64))
            }
            _ => false
        }
    }

    fn less_than(&self, other: &ValueType) -> bool {
        match other.type_id() {
            TypeID::Int2 => {
                self.value.lt(&(cast::<Int2>(other).value() as i32))
            }
            TypeID::Int4 => {
                self.value.lt(&(cast::<Int4>(other).value()))
            }
            TypeID::Int8 => {
                (self.value as i64).lt(&(cast::<Int8>(other).value()))
            }
            TypeID::Float8 => {
                (self.value as f64).lt(&(cast::<Float8>(other).value()))
            }
            TypeID::IPv4 => {
                (self.value as i64).lt(&(cast::<IPv4>(other).value() as i64))
            }
            _ => false
        }
    }

    fn greater_than(&self, other: &ValueType) -> bool {
        match other.type_id() {
            TypeID::Int2 => {
                self.value.gt(&(cast::<Int2>(other).value() as i32))
            }
            TypeID::Int4 => {
                self.value.gt(&(cast::<Int4>(other).value()))
            }
            TypeID::Int8 => {
                (self.value as i64).gt(&(cast::<Int8>(other).value()))
            }
            TypeID::Float8 => {
                (self.value as f64).gt(&(cast::<Float8>(other).value()))
            }
            TypeID::IPv4 => {
                (self.value as i64).gt(&(cast::<IPv4>(other).value() as i64))
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

    fn equals(&self, other: &ValueType) -> bool {
        match other.type_id() {
            TypeID::Int2 => {
                self.value.eq(&(cast::<Int2>(other).value() as i64))
            }
            TypeID::Int4 => {
                self.value.eq(&(cast::<Int4>(other).value() as i64))
            }
            TypeID::Int8 => {
                self.value.eq(&(cast::<Int8>(other).value()))
            }
            TypeID::Float8 => {
                (self.value as f64).eq(&(cast::<Float8>(other).value()))
            }
            TypeID::IPv4 => {
                self.value.eq(&(cast::<IPv4>(other).value() as i64))
            }
            _ => false
        }
    }

    fn less_than(&self, other: &ValueType) -> bool {
        match other.type_id() {
            TypeID::Int2 => {
                self.value.lt(&(cast::<Int2>(other).value() as i64))
            }
            TypeID::Int4 => {
                self.value.lt(&(cast::<Int4>(other).value() as i64))
            }
            TypeID::Int8 => {
                self.value.lt(&(cast::<Int8>(other).value()))
            }
            TypeID::Float8 => {
                (self.value as f64).lt(&(cast::<Float8>(other).value()))
            }
            TypeID::IPv4 => {
                self.value.lt(&(cast::<IPv4>(other).value() as i64))
            }
            _ => false
        }
    }

    fn greater_than(&self, other: &ValueType) -> bool {
        match other.type_id() {
            TypeID::Int2 => {
                self.value.gt(&(cast::<Int2>(other).value() as i64))
            }
            TypeID::Int4 => {
                self.value.gt(&(cast::<Int4>(other).value() as i64))
            }
            TypeID::Int8 => {
                self.value.gt(&(cast::<Int8>(other).value()))
            }
            TypeID::Float8 => {
                (self.value as f64).gt(&(cast::<Float8>(other).value()))
            }
            TypeID::IPv4 => {
                self.value.gt(&(cast::<IPv4>(other).value() as i64))
            }
            _ => false
        }
    }
}

#[cfg(test)]
mod test {
    // TODO: Place unit tests here
}

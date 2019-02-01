extern crate byteorder;

use self::byteorder::{ByteOrder, LittleEndian};

use super::*;

// Define common methods on ip-address types here
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

impl IPAddress for IPv4 {}

impl ValueType for IPv4 {
    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; self.size()];
        LittleEndian::write_u32(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), data)
    }

    fn size(&self) -> usize { 4 }

    fn type_id(&self) -> TypeID {
        TypeID::IPv4
    }

    fn equals(&self, other: &ValueType) -> bool {
        match other.type_id() {
            TypeID::Int2 => {
                (self.value as i64).eq(&(cast::<Int2>(other).value() as i64))
            }
            TypeID::Int4 => {
                (self.value as i64).eq(&(cast::<Int4>(other).value() as i64))
            }
            TypeID::Int8 => {
                (self.value as i64).eq(&(cast::<Int8>(other).value()))
            }
            TypeID::Float8 => {
                (self.value as f64).eq(&(cast::<Float8>(other).value()))
            }
            TypeID::IPv4 => {
                self.value.eq(&(cast::<IPv4>(other).value()))
            }
            _ => false
        }
    }

    fn less_than(&self, other: &ValueType) -> bool {
        match other.type_id() {
            TypeID::Int2 => {
                (self.value as i64).lt(&(cast::<Int2>(other).value() as i64))
            }
            TypeID::Int4 => {
                (self.value as i64).lt(&(cast::<Int4>(other).value() as i64))
            }
            TypeID::Int8 => {
                (self.value as i64).lt(&(cast::<Int8>(other).value()))
            }
            TypeID::Float8 => {
                (self.value as f64).lt(&(cast::<Float8>(other).value()))
            }
            TypeID::IPv4 => {
                self.value.lt(&(cast::<IPv4>(other).value()))
            }
            _ => false
        }
    }

    fn greater_than(&self, other: &ValueType) -> bool {
        match other.type_id() {
            TypeID::Int2 => {
                (self.value as i64).gt(&(cast::<Int2>(other).value() as i64))
            }
            TypeID::Int4 => {
                (self.value as i64).gt(&(cast::<Int4>(other).value() as i64))
            }
            TypeID::Int8 => {
                (self.value as i64).gt(&(cast::<Int8>(other).value()))
            }
            TypeID::Float8 => {
                (self.value as f64).gt(&(cast::<Float8>(other).value()))
            }
            TypeID::IPv4 => {
                self.value.gt(&(cast::<IPv4>(other).value()))
            }
            _ => false
        }
    }
}

#[cfg(test)]
mod test {
    // TODO: Place unit tests here
}
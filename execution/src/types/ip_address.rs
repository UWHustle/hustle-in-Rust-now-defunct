extern crate byteorder;

use self::byteorder::{ByteOrder, LittleEndian};

use super::*;

// Define common methods on ip-address types here
trait IPAddress: ValueType {}

pub struct IPv4 {
    value: u32
}

impl IPv4 {
    pub fn new(value: u32) -> Self {
        IPv4 { value }
    }

    pub fn marshall(data: &[u8]) -> Self {
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

    fn type_id(&self) -> TypeID {
        TypeID::IPv4
    }

    fn compare(&self, other: &ValueType, comp: Comparator) -> bool {
        match other.type_id() {
            TypeID::Int2 => {
                comp.apply(self.value as i64, cast::<Int2>(other).value() as i64)
            }
            TypeID::Int4 => {
                comp.apply(self.value as i64, cast::<Int4>(other).value() as i64)
            }
            TypeID::Int8 => {
                comp.apply(self.value as i64, cast::<Int8>(other).value())
            }
            TypeID::Float4 => {
                comp.apply(self.value as f32, cast::<Float4>(other).value())
            }
            TypeID::Float8 => {
                comp.apply(self.value as f64, cast::<Float8>(other).value())
            }
            TypeID::IPv4 => {
                comp.apply(self.value, cast::<IPv4>(other).value())
            }
            _ => false
        }
    }
}

#[cfg(test)]
mod test {
    // TODO: Place unit tests here
}
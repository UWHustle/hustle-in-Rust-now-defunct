extern crate byteorder;

use self::byteorder::{ByteOrder, LittleEndian};

use super::*;

// Define common methods on integer types here
pub trait Integer: ValueType {}

pub struct Int2 {
    value: i16
}

impl Int2 {
    pub fn marshall(data: &[u8]) -> Self {
        Int2 { value: LittleEndian::read_i16(&data) }
    }
     pub fn new(data:i16)  -> Self {
            Int2 {value :data}
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
    pub fn marshall(data: &[u8]) -> Self {
        Int4 { value: LittleEndian::read_i32(&data) }
    }
    pub fn new(data:i32)  -> Self {
        Int4 {value :data}
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
    pub fn marshall(data: &[u8]) -> Self {
        Int8 { value: LittleEndian::read_i64(&data) }
    }
    pub fn new(data:i64)  -> Self {
        Int8 {value :data}
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
    use self::Int4;
    use super::*;
    use types::integer::Integer;

    #[test]
    fn compare() {
        let int_value_type_1 = Int2 { value: 17 };
        let int_value_type_2 = Int8 { value: 189696 };
        let float_value_type = Float8::new(2456.8374);
        let ipv4_value_type = IPv4::new(0);
        let igbool = int_value_type_1.compare(&int_value_type_2, Comparator::Greater);
        let ilbool = int_value_type_1.compare(&int_value_type_2, Comparator::Less);
        let ieqbool = int_value_type_1.compare(&int_value_type_2, Comparator::Equal);
        assert_eq!(igbool, false);
        assert_eq!(ilbool, true);
        assert_eq!(ieqbool, false);
        let fgbool = int_value_type_1.compare(&float_value_type, Comparator::Greater);
        let flbool = int_value_type_1.compare(&float_value_type, Comparator::Less);
        let feqbool = int_value_type_1.compare(&float_value_type, Comparator::Equal);
        assert_eq!(fgbool, false);
        assert_eq!(flbool, true);
        assert_eq!(feqbool, false);
        let gbool = int_value_type_1.compare(&ipv4_value_type, Comparator::Greater);
        let lbool = int_value_type_1.compare(&ipv4_value_type, Comparator::Less);
        let eqbool = int_value_type_1.compare(&ipv4_value_type, Comparator::Equal);
        assert_eq!(gbool, true);
        assert_eq!(lbool, false);
        assert_eq!(eqbool, false);
    }

}

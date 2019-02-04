extern crate byteorder;

use self::byteorder::{ByteOrder, LittleEndian};

use super::*;

// Define common methods on integer types here
pub trait Integer: ValueType {}

pub struct Int2 {
    value: i16
}

impl Int2 {
    pub fn new(value: i16) -> Self {
        Int2 { value }
    }

    pub fn marshall(data: &[u8]) -> Self {
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
                comp.apply(self.value, cast::<Int2>(other).value())
            }
            TypeID::Int4 => {
                comp.apply(self.value as i32, cast::<Int4>(other).value())
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
                comp.apply(self.value as i64, cast::<IPv4>(other).value() as i64)
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

    pub fn new(data: i32) -> Self {
        Int4 { value: data }
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
                comp.apply(self.value, cast::<Int2>(other).value() as i32)
            }
            TypeID::Int4 => {
                comp.apply(self.value, cast::<Int4>(other).value())
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
                comp.apply(self.value as i64, cast::<IPv4>(other).value() as i64)
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

    pub fn new(data: i64) -> Self {
        Int8 { value: data }
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
                comp.apply(self.value, cast::<Int2>(other).value() as i64)
            }
            TypeID::Int4 => {
                comp.apply(self.value, cast::<Int4>(other).value() as i64)
            }
            TypeID::Int8 => {
                comp.apply(self.value, cast::<Int8>(other).value())
            }
            TypeID::Float4 => {
                comp.apply(self.value as f64, cast::<Float4>(other).value() as f64)
            }
            TypeID::Float8 => {
                comp.apply(self.value as f64, cast::<Float8>(other).value())
            }
            TypeID::IPv4 => {
                comp.apply(self.value, cast::<IPv4>(other).value() as i64)
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
    use types::TypeID;
    use types::integer::Integer;
    use super::BufferType;

    #[test]
    //testing for int2
    fn int2_compare() {
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

    #[test]
    fn int2_type_id() {
        let int2_value_type = Int2 { value: 34 };
        let typeid = int2_value_type.type_id();
        let booltype = match typeid {
            TypeID::Int2 => true,
            _ => false,
        };
        assert_eq!(booltype, true);
    }

    #[test]
    fn int2_unmarshall() {
        let int_value_type = Int2 { value: 56 };
        let owned_buffer = int_value_type.un_marshall();
        let typeid = owned_buffer.type_id();
        let booltype = match typeid {
            TypeID::Int2 => true,
            _ => false,
        };
        let data = owned_buffer.data();
        assert_eq!(56, data[0]);
        assert_eq!(booltype, true);
    }


    //testing for int4
    fn int4_compare() {
        let int_value_type_1 = Int4 { value: 1748 };
        let int_value_type_2 = Int8 { value: 18679696 };
        let int_value_type_3 = Int2 { value: 39 };
        let float_value_type = Float8::new(24256.8374);
        let ipv4_value_type = IPv4::new(123);
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
        let i3gbool = int_value_type_1.compare(&int_value_type_3, Comparator::Greater);
        let i3lbool = int_value_type_1.compare(&int_value_type_3, Comparator::Less);
        let i3eqbool = int_value_type_1.compare(&int_value_type_3, Comparator::Equal);
        assert_eq!(i3gbool, false);
        assert_eq!(i3lbool, true);
        assert_eq!(i3eqbool, false);
    }

    #[test]
    fn int4_type_id() {
        let int4_value_type = Int4 { value: 346 };
        let typeid = int4_value_type.type_id();
        let booltype = match typeid {
            TypeID::Int4 => true,
            _ => false,
        };
        assert_eq!(booltype, true);
    }

    #[test]
    fn int4_unmarshall() {
        let int_value_type = Int4 { value: 2611 };
        let owned_buffer = int_value_type.un_marshall();
        let typeid = owned_buffer.type_id();
        let booltype = match typeid {
            TypeID::Int4 => true,
            _ => false,
        };
        let data = owned_buffer.data();
        assert_eq!(51, data[0]);
        assert_eq!(booltype, true);
    }

    //testing for int8
    fn int8_compare() {
        let int_value_type_1 = Int8 { value: 13784940 };
        let int_value_type_2 = Int4 { value: 18678 };
        let int_value_type_3 = Int2 { value: 100 };
        let float_value_type = Float8::new(7483.7364);
        let ipv4_value_type = IPv4::new(937);
        let igbool = int_value_type_1.compare(&int_value_type_2, Comparator::Greater);
        let ilbool = int_value_type_1.compare(&int_value_type_2, Comparator::Less);
        let ieqbool = int_value_type_1.compare(&int_value_type_2, Comparator::Equal);
        assert_eq!(igbool, true);
        assert_eq!(ilbool, false);
        assert_eq!(ieqbool, false);
        let fgbool = int_value_type_1.compare(&float_value_type, Comparator::Greater);
        let flbool = int_value_type_1.compare(&float_value_type, Comparator::Less);
        let feqbool = int_value_type_1.compare(&float_value_type, Comparator::Equal);
        assert_eq!(fgbool, true);
        assert_eq!(flbool, false);
        assert_eq!(feqbool, false);
        let gbool = int_value_type_1.compare(&ipv4_value_type, Comparator::Greater);
        let lbool = int_value_type_1.compare(&ipv4_value_type, Comparator::Less);
        let eqbool = int_value_type_1.compare(&ipv4_value_type, Comparator::Equal);
        assert_eq!(gbool, true);
        assert_eq!(lbool, false);
        assert_eq!(eqbool, false);
        let i3gbool = int_value_type_1.compare(&int_value_type_3, Comparator::Greater);
        let i3lbool = int_value_type_1.compare(&int_value_type_3, Comparator::Less);
        let i3eqbool = int_value_type_1.compare(&int_value_type_3, Comparator::Equal);
        assert_eq!(i3gbool, true);
        assert_eq!(i3lbool, false);
        assert_eq!(i3eqbool, false);
    }

    #[test]
    fn int8_type_id() {
        let int8_value_type = Int8 { value: 3483646 };
        let typeid = int8_value_type.type_id();
        let booltype = match typeid {
            TypeID::Int8 => true,
            _ => false,
        };
        assert_eq!(booltype, true);
    }

    #[test]
    fn int8_unmarshall() {
        let int_value_type = Int8 { value: 26119474 };
        let owned_buffer = int_value_type.un_marshall();
        let typeid = owned_buffer.type_id();
        let booltype = match typeid {
            TypeID::Int8 => true,
            _ => false,
        };
        let data = owned_buffer.data();
        assert_eq!(50, data[0]);
        assert_eq!(141, data[1]);
        assert_eq!(142, data[2]);
        assert_eq!(1, data[3]);
        assert_eq!(booltype, true);
    }
}

extern crate byteorder;

use self::byteorder::{ByteOrder, LittleEndian};

use super::*;

/// Contains methods common to integer types
pub trait Integer: Numeric {}

/* ============================================================================================== */

/// A 16-bit integer type
#[derive(Clone, Debug)]
pub struct Int2 {
    nullable: bool,
    is_null: bool,
    value: i16,
}

impl Int2 {
    pub fn new(value: i16) -> Self {
        Self {
            nullable: true,
            is_null: false,
            value,
        }
    }

    pub fn create_null() -> Self {
        Self {
            nullable: true,
            is_null: true,
            value: 0,
        }
    }

    pub fn parse(string: &str) -> Self {
        Self::new(string.parse::<i16>().expect("Parsing failed"))
    }

    pub fn marshall(nullable: bool, is_null: bool, data: &[u8]) -> Self {
        Self {
            nullable,
            is_null,
            value: LittleEndian::read_i16(data),
        }
    }

    pub fn value(&self) -> i16 {
        if self.is_null {
            panic!(null_value(self.type_id()));
        }
        self.value
    }
}

impl Integer for Int2 {}

impl Numeric for Int2 {
    fn arithmetic(&self, other: &Numeric, oper: Arithmetic) -> Box<Numeric> {
        let other_cast = match other.type_id().variant {
            Variant::Int2 => {
                cast_numeric::<Int2>(other).value()
            }
            Variant::Int4 => {
                cast_numeric::<Int4>(other).value() as i16
            }
            Variant::Int8 => {
                cast_numeric::<Int8>(other).value() as i16
            }
            Variant::Float4 => {
                cast_numeric::<Float4>(other).value() as i16
            }
            Variant::Float8 => {
                cast_numeric::<Float8>(other).value() as i16
            }
            _ => {
                panic!(not_numeric(other.type_id()))
            }
        };
        let result = oper.apply(self.value, other_cast);
        Box::new(Self::new(result))
    }
}

impl Value for Int2 {
    fn type_id(&self) -> TypeID {
        TypeID::new(Variant::Int2, self.nullable)
    }

    fn is_null(&self) -> bool {
        self.is_null
    }

    fn to_string(&self) -> String {
        if self.is_null {
            String::from("")
        } else {
            self.value.to_string()
        }
    }

    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; self.size()];
        LittleEndian::write_i16(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), self.is_null(), data)
    }

    fn compare(&self, other: &Value, comp: Comparator) -> bool {
        match other.type_id().variant {
            Variant::Int2 => {
                comp.apply(self.value, cast_value::<Int2>(other).value())
            }
            Variant::Int4 => {
                comp.apply(self.value as i32, cast_value::<Int4>(other).value())
            }
            Variant::Int8 => {
                comp.apply(self.value as i64, cast_value::<Int8>(other).value())
            }
            Variant::Float4 => {
                comp.apply(self.value as f32, cast_value::<Float4>(other).value())
            }
            Variant::Float8 => {
                comp.apply(self.value as f64, cast_value::<Float8>(other).value())
            }
            Variant::IPv4 => {
                comp.apply(self.value as i64, cast_value::<IPv4>(other).value() as i64)
            }
            _ => {
                panic!(incomparable(self.type_id(), other.type_id()));
            }
        }
    }
}

/* ============================================================================================== */

/// A 32-bit integer type
#[derive(Clone, Debug)]
pub struct Int4 {
    nullable: bool,
    is_null: bool,
    value: i32,
}

impl Int4 {
    pub fn new(value: i32) -> Self {
        Self {
            nullable: true,
            is_null: false,
            value,
        }
    }

    pub fn create_null() -> Self {
        Self {
            nullable: true,
            is_null: true,
            value: 0,
        }
    }

    pub fn parse(string: &str) -> Self {
        Self::new(string.parse::<i32>().expect("Parsing failed"))
    }

    pub fn marshall(nullable: bool, is_null: bool, data: &[u8]) -> Self {
        Self {
            nullable,
            is_null,
            value: LittleEndian::read_i32(data),
        }
    }

    pub fn value(&self) -> i32 {
        if self.is_null {
            panic!(null_value(self.type_id()));
        }
        self.value
    }
}

impl Integer for Int4 {}

impl Numeric for Int4 {
    fn arithmetic(&self, other: &Numeric, oper: Arithmetic) -> Box<Numeric> {
        let other_cast = match other.type_id().variant {
            Variant::Int2 => {
                cast_numeric::<Int2>(other).value() as i32
            }
            Variant::Int4 => {
                cast_numeric::<Int4>(other).value()
            }
            Variant::Int8 => {
                cast_numeric::<Int8>(other).value() as i32
            }
            Variant::Float4 => {
                cast_numeric::<Float4>(other).value() as i32
            }
            Variant::Float8 => {
                cast_numeric::<Float8>(other).value() as i32
            }
            _ => {
                panic!(not_numeric(other.type_id()))
            }
        };
        let result = oper.apply(self.value, other_cast);
        Box::new(Self::new(result))
    }
}

impl Value for Int4 {
    fn type_id(&self) -> TypeID {
        TypeID::new(Variant::Int4, self.nullable)
    }

    fn is_null(&self) -> bool {
        self.is_null
    }

    fn to_string(&self) -> String {
        if self.is_null {
            String::from("")
        } else {
            self.value.to_string()
        }
    }

    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; self.size()];
        LittleEndian::write_i32(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), self.is_null(), data)
    }

    fn compare(&self, other: &Value, comp: Comparator) -> bool {
        match other.type_id().variant {
            Variant::Int2 => {
                comp.apply(self.value, cast_value::<Int2>(other).value() as i32)
            }
            Variant::Int4 => {
                comp.apply(self.value, cast_value::<Int4>(other).value())
            }
            Variant::Int8 => {
                comp.apply(self.value as i64, cast_value::<Int8>(other).value())
            }
            Variant::Float4 => {
                comp.apply(self.value as f32, cast_value::<Float4>(other).value())
            }
            Variant::Float8 => {
                comp.apply(self.value as f64, cast_value::<Float8>(other).value())
            }
            Variant::IPv4 => {
                comp.apply(self.value as i64, cast_value::<IPv4>(other).value() as i64)
            }
            _ => {
                panic!(incomparable(self.type_id(), other.type_id()));
            }
        }
    }
}

/* ============================================================================================== */

/// A 64-bit integer type
#[derive(Clone, Debug)]
pub struct Int8 {
    nullable: bool,
    is_null: bool,
    value: i64,
}

impl Int8 {
    pub fn new(value: i64) -> Self {
        Self {
            nullable: true,
            is_null: false,
            value,
        }
    }

    pub fn create_null() -> Self {
        Self {
            nullable: true,
            is_null: true,
            value: 0,
        }
    }

    pub fn parse(string: &str) -> Self {
        Self::new(string.parse::<i64>().expect("Parsing failed"))
    }

    pub fn marshall(nullable: bool, is_null: bool, data: &[u8]) -> Self {
        Self {
            nullable,
            is_null,
            value: LittleEndian::read_i64(data),
        }
    }

    pub fn value(&self) -> i64 {
        if self.is_null {
            panic!(null_value(self.type_id()));
        }
        self.value
    }
}

impl Integer for Int8 {}

impl Numeric for Int8 {
    fn arithmetic(&self, other: &Numeric, oper: Arithmetic) -> Box<Numeric> {
        let other_cast = match other.type_id().variant {
            Variant::Int2 => {
                cast_numeric::<Int2>(other).value() as i64
            }
            Variant::Int4 => {
                cast_numeric::<Int4>(other).value() as i64
            }
            Variant::Int8 => {
                cast_numeric::<Int8>(other).value()
            }
            Variant::Float4 => {
                cast_numeric::<Float4>(other).value() as i64
            }
            Variant::Float8 => {
                cast_numeric::<Float8>(other).value() as i64
            }
            _ => {
                panic!(not_numeric(other.type_id()))
            }
        };
        let result = oper.apply(self.value, other_cast);
        Box::new(Self::new(result))
    }
}

impl Value for Int8 {
    fn type_id(&self) -> TypeID {
        TypeID::new(Variant::Int8, self.nullable)
    }

    fn is_null(&self) -> bool {
        self.is_null
    }

    fn to_string(&self) -> String {
        if self.is_null {
            String::from("")
        } else {
            self.value.to_string()
        }
    }

    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; self.size()];
        LittleEndian::write_i64(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), self.is_null(), data)
    }

    fn compare(&self, other: &Value, comp: Comparator) -> bool {
        match other.type_id().variant {
            Variant::Int2 => {
                comp.apply(self.value, cast_value::<Int2>(other).value() as i64)
            }
            Variant::Int4 => {
                comp.apply(self.value, cast_value::<Int4>(other).value() as i64)
            }
            Variant::Int8 => {
                comp.apply(self.value, cast_value::<Int8>(other).value())
            }
            Variant::Float4 => {
                comp.apply(self.value as f64, cast_value::<Float4>(other).value() as f64)
            }
            Variant::Float8 => {
                comp.apply(self.value as f64, cast_value::<Float8>(other).value())
            }
            Variant::IPv4 => {
                comp.apply(self.value, cast_value::<IPv4>(other).value() as i64)
            }
            _ => {
                panic!(incomparable(self.type_id(), other.type_id()));
            }
        }
    }
}

/* ============================================================================================== */

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn int2_type_id() {
        let int2 = Int2::new(56);
        assert_eq!(TypeID::new(Variant::Int2, true), int2.type_id());
    }

    #[test]
    fn int2_un_marshall() {
        let int2_value = Int2::new(56);
        let int2_buffer = int2_value.un_marshall();
        assert_eq!(TypeID::new(Variant::Int2, true), int2_buffer.type_id());

        let data = int2_buffer.data();
        assert_eq!(0x38, data[0]);
        assert_eq!(0x00, data[1]);
    }

    #[test]
    fn int2_compare() {
        let int2 = Int2::new(17);

        let int4 = Int4::new(-1340);
        assert!(!int2.less(&int4));
        assert!(int2.greater(&int4));
        assert!(!int2.equals(&int4));

        let int8 = Int8::new(189696);
        assert!(int2.less(&int8));
        assert!(!int2.greater(&int8));
        assert!(!int2.equals(&int8));

        let float4 = Float4::new(17.0);
        assert!(!int2.less(&float4));
        assert!(!int2.greater(&float4));
        assert!(int2.equals(&float4));

        let float8 = Float8::new(2456.8374);
        assert!(int2.less(&float8));
        assert!(!int2.greater(&float8));
        assert!(!int2.equals(&float8));

        let ipv4 = IPv4::new(0);
        assert!(!int2.less(&ipv4));
        assert!(int2.greater(&ipv4));
        assert!(!int2.equals(&ipv4));
    }

    #[test]
    #[should_panic]
    fn invalid_int2_compare() {
        let int2 = Int2::new(17);
        let utf8_string = UTF8String::new("to life, ");
        assert!(!int2.equals(&utf8_string));
    }

    #[test]
    fn int4_type_id() {
        let int4 = Int4::new(2611);
        assert_eq!(TypeID::new(Variant::Int4, true), int4.type_id());
    }

    #[test]
    fn int4_un_marshall() {
        let int4_value = Int4::new(2611);
        let int4_buffer = int4_value.un_marshall();
        assert_eq!(TypeID::new(Variant::Int4, true), int4_buffer.type_id());

        let data = int4_buffer.data();
        assert_eq!(0x33, data[0]);
        assert_eq!(0x0a, data[1]);
        assert_eq!(0x00, data[2]);
    }

    #[test]
    fn int4_compare() {
        let int4 = Int4::new(1748);

        let int2 = Int2::new(17);
        assert!(!int4.less(&int2));
        assert!(int4.greater(&int2));
        assert!(!int4.equals(&int2));

        let int8 = Int8::new(1748);
        assert!(!int4.less(&int8));
        assert!(!int4.greater(&int8));
        assert!(int4.equals(&int8));

        let float4 = Float4::new(-1234.56);
        assert!(!int4.less(&float4));
        assert!(int4.greater(&float4));
        assert!(!int4.equals(&float4));

        let float8 = Float8::new(24256.8374);
        assert!(int4.less(&float8));
        assert!(!int4.greater(&float8));
        assert!(!int4.equals(&float8));

        let ipv4 = IPv4::new(123);
        assert!(!int4.less(&ipv4));
        assert!(int4.greater(&ipv4));
        assert!(!int4.equals(&ipv4));
    }

    #[test]
    #[should_panic]
    fn invalid_int4_compare() {
        let int4 = Int4::new(1748);
        let utf8_string = UTF8String::new("the universe, ");
        int4.equals(&utf8_string);
    }

    #[test]
    fn int8_type_id() {
        let int8 = Int8::new(3483646);
        assert_eq!(TypeID::new(Variant::Int8, true), int8.type_id());
    }

    #[test]
    fn int8_un_marshall() {
        let int8_value = Int8::new(26119474);
        let int8_buffer = int8_value.un_marshall();
        assert_eq!(TypeID::new(Variant::Int8, true), int8_buffer.type_id());

        let data = int8_buffer.data();
        assert_eq!(0x32, data[0]);
        assert_eq!(0x8d, data[1]);
        assert_eq!(0x8e, data[2]);
        assert_eq!(0x01, data[3]);
        assert_eq!(0x00, data[4]);
    }

    #[test]
    fn int8_compare() {
        let int8 = Int8::new(13784940);

        let int2 = Int2::new(100);
        assert!(!int8.less(&int2));
        assert!(int8.greater(&int2));
        assert!(!int8.equals(&int2));

        let int4 = Int4::new(18678);
        assert!(!int8.less(&int4));
        assert!(int8.greater(&int4));
        assert!(!int8.equals(&int4));

        let float4 = Float4::new(7483.73);
        assert!(!int8.less(&float4));
        assert!(int8.greater(&float4));
        assert!(!int8.equals(&float4));

        let float8 = Float8::new(13784940.0);
        assert!(!int8.less(&float8));
        assert!(!int8.greater(&float8));
        assert!(int8.equals(&float8));

        let ipv4 = IPv4::new(937);
        assert!(!int8.less(&ipv4));
        assert!(int8.greater(&ipv4));
        assert!(!int8.equals(&ipv4));
    }

    #[test]
    #[should_panic]
    fn invalid_int8_compare() {
        let int8 = Int8::new(13784940);
        let utf8_string = UTF8String::new("and everything.");
        int8.equals(&utf8_string);
    }
}

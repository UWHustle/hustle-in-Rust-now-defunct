extern crate byteorder;

use self::byteorder::{ByteOrder, LittleEndian};

use super::*;

/// Contains methods common to integer types
pub trait Integer: Numeric {}

/* ============================================================================================== */

/// An 8-bit integer type
#[derive(Clone, Debug)]
pub struct Int1 {
    value: u8,
    nullable: bool,
    is_null: bool,
}

impl Int1 {
    pub fn new(value: u8, nullable: bool) -> Self {
        Self {
            value,
            nullable,
            is_null: false,
        }
    }

    pub fn from(value: u8) -> Self {
        Self::new(value, true)
    }

    pub fn create_null() -> Self {
        Self {
            value: 0,
            nullable: true,
            is_null: true,
        }
    }

    pub fn parse(string: &str) -> Self {
        Self::from(string.parse::<u8>().expect("Parsing failed"))
    }

    pub fn marshall(data: &[u8], nullable: bool, is_null: bool) -> Self {
        Self {
            value: data[0],
            nullable,
            is_null,
        }
    }

    pub fn value(&self) -> u8 {
        if self.is_null {
            panic!(null_value(self.type_id()));
        }
        self.value
    }
}

impl Integer for Int1 {}

impl Numeric for Int1 {
    fn arithmetic(&self, other: &Numeric, oper: Arithmetic) -> Box<Numeric> {
        let other_cast = match other.type_id().variant {
            Variant::Int1 => cast_numeric::<Int1>(other).value(),
            Variant::Int2 => cast_numeric::<Int2>(other).value() as u8,
            Variant::Int4 => cast_numeric::<Int4>(other).value() as u8,
            Variant::Int8 => cast_numeric::<Int8>(other).value() as u8,
            Variant::Float4 => cast_numeric::<Float4>(other).value() as u8,
            Variant::Float8 => cast_numeric::<Float8>(other).value() as u8,
            _ => panic!(not_numeric(other.type_id())),
        };
        let result = oper.apply(self.value, other_cast);
        Box::new(Self::new(result, self.nullable))
    }
}

impl Value for Int1 {
    fn type_id(&self) -> TypeID {
        TypeID::new(Variant::Int1, self.nullable)
    }

    fn is_null(&self) -> bool {
        self.is_null
    }

    fn to_string(&self) -> String {
        if self.is_null {
            String::new()
        } else {
            self.value.to_string()
        }
    }

    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; self.size()];
        data[0] = self.value;
        OwnedBuffer::new(data, self.type_id(), self.is_null())
    }

    fn compare(&self, other: &Value, comp: Comparator) -> bool {
        match other.type_id().variant {
            Variant::Int1 => {
                comp.apply(self.value, cast_value::<Int1>(other).value())
            }
            Variant::Int2 => {
                comp.apply(i16::from(self.value), cast_value::<Int2>(other).value())
            }
            Variant::Int4 => {
                comp.apply(i32::from(self.value), cast_value::<Int4>(other).value())
            }
            Variant::Int8 => {
                comp.apply(i64::from(self.value), cast_value::<Int8>(other).value())
            }
            Variant::Float4 => {
                comp.apply(f32::from(self.value), cast_value::<Float4>(other).value())
            }
            Variant::Float8 => {
                comp.apply(f64::from(self.value), cast_value::<Float8>(other).value())
            }
            Variant::IPv4 => {
                comp.apply(i64::from(self.value), i64::from(cast_value::<IPv4>(other).value()))
            }
            _ => {
                panic!(incomparable(self.type_id(), other.type_id()));
            }
        }
    }
}

/// A 16-bit integer type
#[derive(Clone, Debug)]
pub struct Int2 {
    value: i16,
    nullable: bool,
    is_null: bool,
}

impl Int2 {
    pub fn new(value: i16, nullable: bool) -> Self {
        Self {
            value,
            nullable,
            is_null: false,
        }
    }

    pub fn from(value: i16) -> Self {
        Self::new(value, true)
    }

    pub fn create_null() -> Self {
        Self {
            value: 0,
            nullable: true,
            is_null: true,
        }
    }

    pub fn parse(string: &str) -> Self {
        Self::from(string.parse::<i16>().expect("Parsing failed"))
    }

    pub fn marshall(data: &[u8], nullable: bool, is_null: bool) -> Self {
        Self {
            value: LittleEndian::read_i16(data),
            nullable,
            is_null,
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
            Variant::Int1 => cast_numeric::<Int1>(other).value() as i16,
            Variant::Int2 => cast_numeric::<Int2>(other).value(),
            Variant::Int4 => cast_numeric::<Int4>(other).value() as i16,
            Variant::Int8 => cast_numeric::<Int8>(other).value() as i16,
            Variant::Float4 => cast_numeric::<Float4>(other).value() as i16,
            Variant::Float8 => cast_numeric::<Float8>(other).value() as i16,
            _ => panic!(not_numeric(other.type_id())),
        };
        let result = oper.apply(self.value, other_cast);
        Box::new(Self::new(result, self.nullable))
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
            String::new()
        } else {
            self.value.to_string()
        }
    }

    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; self.size()];
        LittleEndian::write_i16(&mut data, self.value);
        OwnedBuffer::new(data, self.type_id(), self.is_null())
    }

    fn compare(&self, other: &Value, comp: Comparator) -> bool {
        match other.type_id().variant {
            Variant::Int1 => {
                comp.apply(self.value, i16::from(cast_value::<Int1>(other).value()))
            }
            Variant::Int2 => {
                comp.apply(self.value, cast_value::<Int2>(other).value())
            }
            Variant::Int4 => {
                comp.apply(i32::from(self.value), cast_value::<Int4>(other).value())
            }
            Variant::Int8 => {
                comp.apply(i64::from(self.value), cast_value::<Int8>(other).value())
            }
            Variant::Float4 => {
                comp.apply(f32::from(self.value), cast_value::<Float4>(other).value())
            }
            Variant::Float8 => {
                comp.apply(f64::from(self.value), cast_value::<Float8>(other).value())
            }
            Variant::IPv4 => {
                comp.apply(i64::from(self.value), i64::from(cast_value::<IPv4>(other).value()))
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
    value: i32,
    nullable: bool,
    is_null: bool,
}

impl Int4 {
    pub fn new(value: i32, nullable: bool) -> Self {
        Self {
            value,
            nullable,
            is_null: false,
        }
    }

    pub fn from(value: i32) -> Self {
        Self::new(value, true)
    }

    pub fn create_null() -> Self {
        Self {
            value: 0,
            nullable: true,
            is_null: true,
        }
    }

    pub fn parse(string: &str) -> Self {
        Self::from(string.parse::<i32>().expect("Parsing failed"))
    }

    pub fn marshall(data: &[u8], nullable: bool, is_null: bool) -> Self {
        Self {
            value: LittleEndian::read_i32(data),
            nullable,
            is_null,
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
            Variant::Int1 => i32::from(cast_numeric::<Int1>(other).value()),
            Variant::Int2 => i32::from(cast_numeric::<Int2>(other).value()),
            Variant::Int4 => cast_numeric::<Int4>(other).value(),
            Variant::Int8 => cast_numeric::<Int8>(other).value() as i32,
            Variant::Float4 => cast_numeric::<Float4>(other).value() as i32,
            Variant::Float8 => cast_numeric::<Float8>(other).value() as i32,
            _ => panic!(not_numeric(other.type_id())),
        };
        let result = oper.apply(self.value, other_cast);
        Box::new(Self::new(result, self.nullable))
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
            String::new()
        } else {
            self.value.to_string()
        }
    }

    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; self.size()];
        LittleEndian::write_i32(&mut data, self.value);
        OwnedBuffer::new(data, self.type_id(), self.is_null())
    }

    fn compare(&self, other: &Value, comp: Comparator) -> bool {
        match other.type_id().variant {
            Variant::Int1 => {
                comp.apply(self.value, i32::from(cast_value::<Int1>(other).value()))
            }
            Variant::Int2 => {
                comp.apply(self.value, i32::from(cast_value::<Int2>(other).value()))
            }
            Variant::Int4 => {
                comp.apply(self.value, cast_value::<Int4>(other).value())
            }
            Variant::Int8 => {
                comp.apply(i64::from(self.value), cast_value::<Int8>(other).value())
            }
            Variant::Float4 => {
                comp.apply(self.value as f32, cast_value::<Float4>(other).value())
            }
            Variant::Float8 => {
                comp.apply(f64::from(self.value), cast_value::<Float8>(other).value())
            }
            Variant::IPv4 => {
                comp.apply(i64::from(self.value), i64::from(cast_value::<IPv4>(other).value()))
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
    pub fn new(value: i64, nullable: bool) -> Self {
        Self {
            value,
            nullable,
            is_null: false,
        }
    }

    pub fn from(value: i64) -> Self {
        Self::new(value, true)
    }

    pub fn create_null() -> Self {
        Self {
            value: 0,
            nullable: true,
            is_null: true,
        }
    }

    pub fn parse(string: &str) -> Self {
        Self::from(string.parse::<i64>().expect("Parsing failed"))
    }

    pub fn marshall(data: &[u8], nullable: bool, is_null: bool) -> Self {
        Self {
            value: LittleEndian::read_i64(data),
            nullable,
            is_null,
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
            Variant::Int1 => i64::from(cast_numeric::<Int1>(other).value()),
            Variant::Int2 => i64::from(cast_numeric::<Int2>(other).value()),
            Variant::Int4 => i64::from(cast_numeric::<Int4>(other).value()),
            Variant::Int8 => cast_numeric::<Int8>(other).value(),
            Variant::Float4 => cast_numeric::<Float4>(other).value() as i64,
            Variant::Float8 => cast_numeric::<Float8>(other).value() as i64,
            _ => panic!(not_numeric(other.type_id())),
        };
        let result = oper.apply(self.value, other_cast);
        Box::new(Self::new(result, self.nullable))
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
            String::new()
        } else {
            self.value.to_string()
        }
    }

    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; self.size()];
        LittleEndian::write_i64(&mut data, self.value);
        OwnedBuffer::new(data, self.type_id(), self.is_null())
    }

    fn compare(&self, other: &Value, comp: Comparator) -> bool {
        match other.type_id().variant {
            Variant::Int1 => {
                comp.apply(self.value, i64::from(cast_value::<Int1>(other).value()))
            }
            Variant::Int2 => {
                comp.apply(self.value, i64::from(cast_value::<Int2>(other).value()))
            }
            Variant::Int4 => {
                comp.apply(self.value, i64::from(cast_value::<Int4>(other).value()))
            }
            Variant::Int8 => {
                comp.apply(self.value, cast_value::<Int8>(other).value())
            }
            Variant::Float4 => {
                comp.apply(self.value as f64, f64::from(cast_value::<Float4>(other).value()))
            }
            Variant::Float8 => {
                comp.apply(self.value as f64, cast_value::<Float8>(other).value())
            }
            Variant::IPv4 => {
                comp.apply(self.value, i64::from(cast_value::<IPv4>(other).value()))
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
    fn int1_type_id() {
        let int1 = Int1::from(11);
        assert_eq!(TypeID::new(Variant::Int1, true), int1.type_id());
    }

    #[test]
    fn int1_un_marshall() {
        let int1_value = Int1::from(11);
        let int1_buffer = int1_value.un_marshall();
        assert_eq!(TypeID::new(Variant::Int1, true), int1_buffer.type_id());

        let data = int1_buffer.data();
        assert_eq!(0x0b, data[0]);
    }

    #[test]
    fn int1_compare() {
        let int1 = Int1::from(123);

        let int2 = Int2::from(1311);
        assert!(int1.less(&int2));
        assert!(!int1.greater(&int2));
        assert!(!int1.equals(&int2));

        let int4 = Int4::from(-2233);
        assert!(!int1.less(&int4));
        assert!(int1.greater(&int4));
        assert!(!int1.equals(&int4));

        let int8 = Int8::from(123);
        assert!(!int1.less(&int8));
        assert!(!int1.greater(&int8));
        assert!(int1.equals(&int8));

        let float4 = Float4::from(123.0);
        assert!(!int1.less(&float4));
        assert!(!int1.greater(&float4));
        assert!(int1.equals(&float4));

        let float8 = Float8::from(8889996.0);
        assert!(int1.less(&float8));
        assert!(!int1.greater(&float8));
        assert!(!int1.equals(&float8));

        let ipv4 = IPv4::from(122);
        assert!(int1.greater(&ipv4));
        assert!(!int1.less(&ipv4));
        assert!(!int1.equals(&ipv4));
    }

    #[test]
    #[should_panic]
    fn invalid_int1_compare() {
        let int1 = Int1::from(123);
        let utf8_string = UTF8String::from("late arrival");
        assert!(!int1.equals(&utf8_string));
    }

    #[test]
    fn int2_type_id() {
        let int2 = Int2::from(56);
        assert_eq!(TypeID::new(Variant::Int2, true), int2.type_id());
    }

    #[test]
    fn int2_un_marshall() {
        let int2_value = Int2::from(56);
        let int2_buffer = int2_value.un_marshall();
        assert_eq!(TypeID::new(Variant::Int2, true), int2_buffer.type_id());

        let data = int2_buffer.data();
        assert_eq!(0x38, data[0]);
        assert_eq!(0x00, data[1]);
    }

    #[test]
    fn int2_compare() {
        let int2 = Int2::from(17);

        let int1 = Int1::from(255);
        assert!(int2.less(&int1));
        assert!(!int2.greater(&int1));
        assert!(!int2.equals(&int1));

        let int4 = Int4::from(-1340);
        assert!(!int2.less(&int4));
        assert!(int2.greater(&int4));
        assert!(!int2.equals(&int4));

        let int8 = Int8::from(189696);
        assert!(int2.less(&int8));
        assert!(!int2.greater(&int8));
        assert!(!int2.equals(&int8));

        let float4 = Float4::from(17.0);
        assert!(!int2.less(&float4));
        assert!(!int2.greater(&float4));
        assert!(int2.equals(&float4));

        let float8 = Float8::from(2456.8374);
        assert!(int2.less(&float8));
        assert!(!int2.greater(&float8));
        assert!(!int2.equals(&float8));

        let ipv4 = IPv4::from(0);
        assert!(!int2.less(&ipv4));
        assert!(int2.greater(&ipv4));
        assert!(!int2.equals(&ipv4));
    }

    #[test]
    #[should_panic]
    fn invalid_int2_compare() {
        let int2 = Int2::from(17);
        let utf8_string = UTF8String::from("to life, ");
        assert!(!int2.equals(&utf8_string));
    }

    #[test]
    fn int4_type_id() {
        let int4 = Int4::from(2611);
        assert_eq!(TypeID::new(Variant::Int4, true), int4.type_id());
    }

    #[test]
    fn int4_un_marshall() {
        let int4_value = Int4::from(2611);
        let int4_buffer = int4_value.un_marshall();
        assert_eq!(TypeID::new(Variant::Int4, true), int4_buffer.type_id());

        let data = int4_buffer.data();
        assert_eq!(0x33, data[0]);
        assert_eq!(0x0a, data[1]);
        assert_eq!(0x00, data[2]);
    }

    #[test]
    fn int4_compare() {
        let int4 = Int4::from(1748);

        let int1 = Int1::from(0);
        assert!(!int4.less(&int1));
        assert!(int4.greater(&int1));
        assert!(!int4.equals(&int1));

        let int2 = Int2::from(17);
        assert!(!int4.less(&int2));
        assert!(int4.greater(&int2));
        assert!(!int4.equals(&int2));

        let int8 = Int8::from(1748);
        assert!(!int4.less(&int8));
        assert!(!int4.greater(&int8));
        assert!(int4.equals(&int8));

        let float4 = Float4::from(-1234.56);
        assert!(!int4.less(&float4));
        assert!(int4.greater(&float4));
        assert!(!int4.equals(&float4));

        let float8 = Float8::from(24256.8374);
        assert!(int4.less(&float8));
        assert!(!int4.greater(&float8));
        assert!(!int4.equals(&float8));

        let ipv4 = IPv4::from(123);
        assert!(!int4.less(&ipv4));
        assert!(int4.greater(&ipv4));
        assert!(!int4.equals(&ipv4));
    }

    #[test]
    #[should_panic]
    fn invalid_int4_compare() {
        let int4 = Int4::from(1748);
        let utf8_string = UTF8String::from("the universe, ");
        int4.equals(&utf8_string);
    }

    #[test]
    fn int8_type_id() {
        let int8 = Int8::from(3483646);
        assert_eq!(TypeID::new(Variant::Int8, true), int8.type_id());
    }

    #[test]
    fn int8_un_marshall() {
        let int8_value = Int8::from(26119474);
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
        let int8 = Int8::from(13784940);

        let int1 = Int1::from(11);
        assert!(!int8.less(&int1));
        assert!(int8.greater(&int1));
        assert!(!int8.equals(&int1));

        let int2 = Int2::from(100);
        assert!(!int8.less(&int2));
        assert!(int8.greater(&int2));
        assert!(!int8.equals(&int2));

        let int4 = Int4::from(18678);
        assert!(!int8.less(&int4));
        assert!(int8.greater(&int4));
        assert!(!int8.equals(&int4));

        let float4 = Float4::from(7483.73);
        assert!(!int8.less(&float4));
        assert!(int8.greater(&float4));
        assert!(!int8.equals(&float4));

        let float8 = Float8::from(13784940.0);
        assert!(!int8.less(&float8));
        assert!(!int8.greater(&float8));
        assert!(int8.equals(&float8));

        let ipv4 = IPv4::from(937);
        assert!(!int8.less(&ipv4));
        assert!(int8.greater(&ipv4));
        assert!(!int8.equals(&ipv4));
    }

    #[test]
    #[should_panic]
    fn invalid_int8_compare() {
        let int8 = Int8::from(13784940);
        let utf8_string = UTF8String::from("and everything.");
        int8.equals(&utf8_string);
    }
}

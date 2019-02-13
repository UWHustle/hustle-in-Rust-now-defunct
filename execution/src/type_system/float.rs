extern crate byteorder;

use self::byteorder::{ByteOrder, LittleEndian};

use super::*;

/// Contains methods common to floating-point types
pub trait Float: Numeric {}

/* ============================================================================================== */

/// A 32-bit floating point type
#[derive(Clone, Debug)]
pub struct Float4 {
    nullable: bool,
    is_null: bool,
    value: f32,
}

impl Float4 {
    pub fn new(value: f32) -> Self {
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
            value: 0.0,
        }
    }

    pub fn parse(string: &str) -> Self {
        Self::new(string.parse::<f32>().expect("Parsing failed"))
    }

    pub fn marshall(nullable: bool, is_null: bool, data: &[u8]) -> Self {
        Self {
            nullable,
            is_null,
            value: LittleEndian::read_f32(data),
        }
    }

    pub fn value(&self) -> f32 {
        if self.is_null {
            panic!(null_value(self.type_id()));
        }
        self.value
    }
}

impl Float for Float4 {}

impl Numeric for Float4 {
    fn arithmetic(&self, other: &Numeric, oper: Arithmetic) -> Box<Numeric> {
        let other_cast = match other.type_id().variant {
            Variant::Int2 => {
                 cast_numeric::<Int2>(other).value() as f32
            }
            Variant::Int4 => {
                cast_numeric::<Int4>(other).value() as f32
            }
            Variant::Int8 => {
                cast_numeric::<Int8>(other).value() as f32
            }
            Variant::Float4 => {
                cast_numeric::<Float4>(other).value()
            }
            Variant::Float8 => {
                cast_numeric::<Float8>(other).value() as f32
            }
            _ => {
                panic!(not_numeric(other.type_id()))
            }
        };
        let result = oper.apply(self.value, other_cast);
        Box::new(Self::new(result))
    }
}

impl Value for Float4 {
    fn type_id(&self) -> TypeID {
        TypeID::new(Variant::Float4, self.nullable)
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
        LittleEndian::write_f32(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), self.is_null(), data)
    }

    fn compare(&self, other: &Value, comp: Comparator) -> bool {
        match other.type_id().variant {
            Variant::Int2 => {
                comp.apply(self.value, cast_value::<Int2>(other).value() as f32)
            }
            Variant::Int4 => {
                comp.apply(self.value, cast_value::<Int4>(other).value() as f32)
            }
            Variant::Int8 => {
                comp.apply(self.value as f64, cast_value::<Int8>(other).value() as f64)
            }
            Variant::Float4 => {
                comp.apply(self.value, cast_value::<Float4>(other).value())
            }
            Variant::Float8 => {
                comp.apply(self.value as f64, cast_value::<Float8>(other).value())
            }
            Variant::IPv4 => {
                comp.apply(self.value, cast_value::<IPv4>(other).value() as f32)
            }
            _ => {
                panic!(incomparable(self.type_id(), other.type_id()));
            }
        }
    }
}

/* ============================================================================================== */

/// A 64-bit floating point type
#[derive(Clone, Debug)]
pub struct Float8 {
    nullable: bool,
    is_null: bool,
    value: f64,
}

impl Float8 {
    pub fn new(value: f64) -> Self {
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
            value: 0.0,
        }
    }

    pub fn parse(string: &str) -> Self {
        Self::new(string.parse::<f64>().expect("Parsing failed"))
    }

    pub fn marshall(nullable: bool, is_null: bool, data: &[u8]) -> Self {
        Self {
            nullable,
            is_null,
            value: LittleEndian::read_f64(data),
        }
    }

    pub fn value(&self) -> f64 {
        if self.is_null {
            panic!(null_value(self.type_id()));
        }
        self.value
    }
}

impl Float for Float8 {}

impl Numeric for Float8 {
    fn arithmetic(&self, other: &Numeric, oper: Arithmetic) -> Box<Numeric> {
        let other_cast = match other.type_id().variant {
            Variant::Int2 => {
                cast_numeric::<Int2>(other).value() as f64
            }
            Variant::Int4 => {
                cast_numeric::<Int4>(other).value() as f64
            }
            Variant::Int8 => {
                cast_numeric::<Int8>(other).value() as f64
            }
            Variant::Float4 => {
                cast_numeric::<Float4>(other).value() as f64
            }
            Variant::Float8 => {
                cast_numeric::<Float8>(other).value()
            }
            _ => {
                panic!(not_numeric(other.type_id()))
            }
        };
        let result = oper.apply(self.value, other_cast);
        Box::new(Self::new(result))
    }
}

impl Value for Float8 {
    fn type_id(&self) -> TypeID {
        TypeID::new(Variant::Float8, self.nullable)
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
        LittleEndian::write_f64(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), self.is_null(), data)
    }

    fn compare(&self, other: &Value, comp: Comparator) -> bool {
        match other.type_id().variant {
            Variant::Int2 => {
                comp.apply(self.value, cast_value::<Int2>(other).value() as f64)
            }
            Variant::Int4 => {
                comp.apply(self.value, cast_value::<Int4>(other).value() as f64)
            }
            Variant::Int8 => {
                comp.apply(self.value, cast_value::<Int8>(other).value() as f64)
            }
            Variant::Float4 => {
                comp.apply(self.value, cast_value::<Float4>(other).value() as f64)
            }
            Variant::Float8 => {
                comp.apply(self.value, cast_value::<Float8>(other).value())
            }
            Variant::IPv4 => {
                comp.apply(self.value, cast_value::<IPv4>(other).value() as f64)
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
    fn float4_type_id() {
        let float4 = Float4::new(13.7);
        assert_eq!(TypeID::new(Variant::Float4, true), float4.type_id());
    }

    #[test]
    fn float4_un_marshall() {
        let float4_value = Float4::new(13.7);
        let float4_buffer = float4_value.un_marshall();
        assert_eq!(TypeID::new(Variant::Float4, true), float4_buffer.type_id());

        let data = float4_buffer.data();
        assert_eq!(0x41, data[0]);
        assert_eq!(0x5b, data[1]);
        assert_eq!(0x33, data[2]);
        assert_eq!(0x33, data[3]);
    }

    #[test]
    fn float4_compare() {
        let float4 = Float4::new(1843.314);

        let int2 = Int2::new(1843);
        assert!(!float4.less(&int2));
        assert!(float4.greater(&int2));
        assert!(!float4.equals(&int2));

        let int4 = Int4::new(1844);
        assert!(float4.less(&int4));
        assert!(!float4.greater(&int4));
        assert!(!float4.equals(&int4));

        let int8 = Int8::new(-123456789);
        assert!(!float4.less(&int8));
        assert!(float4.greater(&int8));
        assert!(!float4.equals(&int8));

        let float8 = Float8::new(1843.314);
        assert!(!float4.less(&float8));
        assert!(!float4.greater(&float8));
        assert!(float4.equals(&float8));

        let ipv4 = IPv4::new(1000);
        assert!(!float4.less(&ipv4));
        assert!(float4.greater(&ipv4));
        assert!(!float4.equals(&ipv4));

        let utf8_string = UTF8String::new("Forty two: ");
        assert!(!float4.less(&utf8_string));
        assert!(!float4.greater(&utf8_string));
        assert!(!float4.equals(&utf8_string));
    }

    #[test]
    fn float8_type_id() {
        let float8 = Float8::new(12228.444);
        assert_eq!(TypeID::new(Variant::Float8, true), float8.type_id());
    }

    #[test]
    fn float8_un_marshall() {
        let float8_value = Float8::new(12228.444);
        let float8_buffer = float8_value.un_marshall();
        assert_eq!(TypeID::new(Variant::Float8, true), float8_buffer.type_id());

        let data = float8_buffer.data();
        assert_eq!(0x40, data[0]);
        assert_eq!(0xc7, data[1]);
        assert_eq!(0xe2, data[2]);
        assert_eq!(0x38, data[3]);
        assert_eq!(0xd4, data[4]);
        assert_eq!(0xfd, data[5]);
        assert_eq!(0xf3, data[6]);
        assert_eq!(0xb6, data[7]);
    }

    #[test]
    fn float8_compare() {
        let float8 = Float8::new(987654321.0);

        let int2 = Int2::new(-10);
        assert!(!float8.less(&int2));
        assert!(float8.greater(&int2));
        assert!(!float8.equals(&int2));

        let int4 = Int4::new(180000);
        assert!(!float8.less(&int4));
        assert!(float8.greater(&int4));
        assert!(!float8.equals(&int4));

        let int8 = Int4::new(987654321);
        assert!(!float8.less(&int8));
        assert!(!float8.greater(&int8));
        assert!(float8.equals(&int8));

        let float4 = Float4::new(10987654321.0);
        assert!(float8.less(&float4));
        assert!(!float8.greater(&float4));
        assert!(!float8.equals(&float4));

        let ipv4 = IPv4::new(999);
        assert!(!float8.less(&ipv4));
        assert!(float8.greater(&ipv4));
        assert!(!float8.equals(&ipv4));

        let utf8_string = UTF8String::new("the answer ");
        assert!(!float8.less(&utf8_string));
        assert!(!float8.greater(&utf8_string));
        assert!(!float8.equals(&utf8_string));
    }
}
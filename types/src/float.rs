extern crate byteorder;

use self::byteorder::{ByteOrder, LittleEndian};

use super::*;

/// Contains methods common to floating-point types
pub trait Float: Numeric {}

/* ============================================================================================== */

/// A 32-bit floating point type
#[derive(Clone, Debug)]
pub struct Float4 {
    value: f32,
    nullable: bool,
    is_null: bool,
}

impl Float4 {
    pub fn new(value: f32, nullable: bool) -> Self {
        Self {
            value,
            nullable,
            is_null: false,
        }
    }

    pub fn from(value: f32) -> Self {
        Self::new(value, true)
    }

    pub fn create_null() -> Self {
        Self {
            value: 0.0,
            nullable: true,
            is_null: true,
        }
    }

    pub fn parse(string: &str) -> Result<Self, String> {
        match string.parse::<f32>() {
            Ok(val) => Ok(Self::from(val)),
            Err(err) => Err(err.to_string()),
        }
    }

    pub fn marshall(data: &[u8], nullable: bool, is_null: bool) -> Self {
        Self {
            value: LittleEndian::read_f32(data),
            nullable,
            is_null,
        }
    }

    pub fn value(&self) -> f32 {
        if self.is_null {
            panic!(null_value(self.data_type()));
        }
        self.value
    }
}

impl Float for Float4 {}

impl Numeric for Float4 {
    fn arithmetic(&self, other: &Numeric, oper: Arithmetic) -> Box<Numeric> {
        let other_cast = match other.data_type().variant {
            Variant::Int1 => f32::from(cast_numeric::<Int1>(other).value()),
            Variant::Int2 => f32::from(cast_numeric::<Int2>(other).value()),
            Variant::Int4 => cast_numeric::<Int4>(other).value() as f32,
            Variant::Int8 => cast_numeric::<Int8>(other).value() as f32,
            Variant::Float4 => cast_numeric::<Float4>(other).value(),
            Variant::Float8 => cast_numeric::<Float8>(other).value() as f32,
            _ => panic!(not_numeric(other.data_type())),
        };
        let result = oper.apply(self.value, other_cast);
        Box::new(Self::new(result, self.nullable))
    }
}

impl Value for Float4 {
    fn data_type(&self) -> DataType {
        DataType::new(Variant::Float4, self.nullable)
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
        LittleEndian::write_f32(&mut data, self.value);
        OwnedBuffer::new(data, self.data_type(), self.is_null())
    }

    fn compare(&self, other: &Value, comp: Comparator) -> bool {
        match other.data_type().variant {
            Variant::Int1 => comp.apply(self.value, f32::from(cast_value::<Int1>(other).value())),
            Variant::Int2 => comp.apply(self.value, f32::from(cast_value::<Int2>(other).value())),
            Variant::Int4 => comp.apply(self.value, cast_value::<Int4>(other).value() as f32),
            Variant::Int8 => comp.apply(
                f64::from(self.value),
                cast_value::<Int8>(other).value() as f64,
            ),
            Variant::Float4 => comp.apply(self.value, cast_value::<Float4>(other).value()),
            Variant::Float8 => {
                comp.apply(f64::from(self.value), cast_value::<Float8>(other).value())
            }
            Variant::IPv4 => comp.apply(self.value, cast_value::<IPv4>(other).value() as f32),
            _ => panic!(incomparable(self.data_type(), other.data_type())),
        }
    }
}

/* ============================================================================================== */

/// A 64-bit floating point type
#[derive(Clone, Debug)]
pub struct Float8 {
    value: f64,
    nullable: bool,
    is_null: bool,
}

impl Float8 {
    pub fn new(value: f64, nullable: bool) -> Self {
        Self {
            value,
            nullable,
            is_null: false,
        }
    }

    pub fn from(value: f64) -> Self {
        Self::new(value, true)
    }

    pub fn create_null() -> Self {
        Self {
            value: 0.0,
            nullable: true,
            is_null: true,
        }
    }

    pub fn parse(string: &str) -> Result<Self, String> {
        match string.parse::<f64>() {
            Ok(val) => Ok(Self::from(val)),
            Err(err) => Err(err.to_string()),
        }
    }

    pub fn marshall(data: &[u8], nullable: bool, is_null: bool) -> Self {
        Self {
            value: LittleEndian::read_f64(data),
            nullable,
            is_null,
        }
    }

    pub fn value(&self) -> f64 {
        if self.is_null {
            panic!(null_value(self.data_type()));
        }
        self.value
    }
}

impl Float for Float8 {}

impl Numeric for Float8 {
    fn arithmetic(&self, other: &Numeric, oper: Arithmetic) -> Box<Numeric> {
        let other_cast = match other.data_type().variant {
            Variant::Int1 => f64::from(cast_numeric::<Int1>(other).value()),
            Variant::Int2 => f64::from(cast_numeric::<Int2>(other).value()),
            Variant::Int4 => f64::from(cast_numeric::<Int4>(other).value()),
            Variant::Int8 => cast_numeric::<Int8>(other).value() as f64,
            Variant::Float4 => f64::from(cast_numeric::<Float4>(other).value()),
            Variant::Float8 => cast_numeric::<Float8>(other).value(),
            _ => panic!(not_numeric(other.data_type())),
        };
        let result = oper.apply(self.value, other_cast);
        Box::new(Self::new(result, self.nullable))
    }
}

impl Value for Float8 {
    fn data_type(&self) -> DataType {
        DataType::new(Variant::Float8, self.nullable)
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
        LittleEndian::write_f64(&mut data, self.value);
        OwnedBuffer::new(data, self.data_type(), self.is_null())
    }

    fn compare(&self, other: &Value, comp: Comparator) -> bool {
        match other.data_type().variant {
            Variant::Int1 => comp.apply(self.value, f64::from(cast_value::<Int1>(other).value())),
            Variant::Int2 => comp.apply(self.value, f64::from(cast_value::<Int2>(other).value())),
            Variant::Int4 => comp.apply(self.value, f64::from(cast_value::<Int4>(other).value())),
            Variant::Int8 => comp.apply(self.value, cast_value::<Int8>(other).value() as f64),
            Variant::Float4 => {
                comp.apply(self.value, f64::from(cast_value::<Float4>(other).value()))
            }
            Variant::Float8 => comp.apply(self.value, cast_value::<Float8>(other).value()),
            Variant::IPv4 => comp.apply(self.value, f64::from(cast_value::<IPv4>(other).value())),
            _ => {
                panic!(incomparable(self.data_type(), other.data_type()));
            }
        }
    }
}

/* ============================================================================================== */

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn float4_data_type() {
        let float4 = Float4::from(13.7);
        assert_eq!(DataType::new(Variant::Float4, true), float4.data_type());
    }

    #[test]
    fn float4_compare() {
        let float4 = Float4::from(1843.5);

        let int1 = Int1::from(11);
        assert!(!float4.less(&int1));
        assert!(float4.greater(&int1));
        assert!(!float4.equals(&int1));

        let int2 = Int2::from(1843);
        assert!(!float4.less(&int2));
        assert!(float4.greater(&int2));
        assert!(!float4.equals(&int2));

        let int4 = Int4::from(1844);
        assert!(float4.less(&int4));
        assert!(!float4.greater(&int4));
        assert!(!float4.equals(&int4));

        let int8 = Int8::from(-123456789);
        assert!(!float4.less(&int8));
        assert!(float4.greater(&int8));
        assert!(!float4.equals(&int8));

        let float8 = Float8::from(1843.5);
        assert!(!float4.less(&float8));
        assert!(!float4.greater(&float8));
        assert!(float4.equals(&float8));

        let ipv4 = IPv4::from(1000);
        assert!(!float4.less(&ipv4));
        assert!(float4.greater(&ipv4));
        assert!(!float4.equals(&ipv4));
    }

    #[test]
    fn float4_un_marshall() {
        let float4_value = Float4::from(13.7);
        let float4_buffer = float4_value.un_marshall();
        assert_eq!(
            DataType::new(Variant::Float4, true),
            float4_buffer.data_type()
        );

        let data = float4_buffer.data();
        assert_eq!(0x33, data[0]);
        assert_eq!(0x33, data[1]);
        assert_eq!(0x5b, data[2]);
        assert_eq!(0x41, data[3]);
    }

    #[test]
    #[should_panic]
    fn invalid_float4_compare() {
        let float4 = Float4::from(1843.5);
        let utf8_string = ByteString::from("Forty two: ");
        float4.equals(&utf8_string);
    }

    #[test]
    fn float8_data_type() {
        let float8 = Float8::from(12228.444);
        assert_eq!(DataType::new(Variant::Float8, true), float8.data_type());
    }

    #[test]
    fn float8_un_marshall() {
        let float8_value = Float8::from(12228.444);
        let float8_buffer = float8_value.un_marshall();
        assert_eq!(
            DataType::new(Variant::Float8, true),
            float8_buffer.data_type()
        );

        let data = float8_buffer.data();
        assert_eq!(0xb6, data[0]);
        assert_eq!(0xf3, data[1]);
        assert_eq!(0xfd, data[2]);
        assert_eq!(0xd4, data[3]);
        assert_eq!(0x38, data[4]);
        assert_eq!(0xe2, data[5]);
        assert_eq!(0xc7, data[6]);
        assert_eq!(0x40, data[7]);
    }

    #[test]
    fn float8_compare() {
        let float8 = Float8::from(987654321.0);

        let int1 = Int1::from(11);
        assert!(!float8.less(&int1));
        assert!(float8.greater(&int1));
        assert!(!float8.equals(&int1));

        let int2 = Int2::from(-10);
        assert!(!float8.less(&int2));
        assert!(float8.greater(&int2));
        assert!(!float8.equals(&int2));

        let int4 = Int4::from(180000);
        assert!(!float8.less(&int4));
        assert!(float8.greater(&int4));
        assert!(!float8.equals(&int4));

        let int8 = Int4::from(987654321);
        assert!(!float8.less(&int8));
        assert!(!float8.greater(&int8));
        assert!(float8.equals(&int8));

        let float4 = Float4::from(10987654321.0);
        assert!(float8.less(&float4));
        assert!(!float8.greater(&float4));
        assert!(!float8.equals(&float4));

        let ipv4 = IPv4::from(999);
        assert!(!float8.less(&ipv4));
        assert!(float8.greater(&ipv4));
        assert!(!float8.equals(&ipv4));
    }

    #[test]
    #[should_panic]
    fn invalid_float8_compare() {
        let float8 = Float8::from(987654321.0);
        let utf8_string = ByteString::from("the answer ");
        float8.equals(&utf8_string);
    }
}
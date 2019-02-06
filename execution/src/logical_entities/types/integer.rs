extern crate byteorder;

use self::byteorder::{ByteOrder, LittleEndian};

use super::*;

// Define common methods on integer types here
pub trait Integer: Numeric {}

#[derive(Clone)]
pub struct Int2 {
    nullable: bool,
    is_null: bool,
    value: i16,
}

impl Int2 {
    // Note that this assumes the type is nullable
    pub fn new(value: i16) -> Self {
        Int2 {
            nullable: true,
            is_null: false,
            value,
        }
    }

    pub fn marshall(nullable: bool, buffer: &BufferType) -> Self {
        Int2 {
            nullable,
            is_null: buffer.is_null(),
            value: LittleEndian::read_i16(buffer.data()),
        }
    }

    pub fn value(&self) -> i16 {
        if self.is_null {
            panic!("Attempting to retrieve i16 value of null Int2");
        }
        self.value
    }
}

impl Integer for Int2 {}

impl Numeric for Int2 {
    fn add(&self, other: &Numeric) -> Box<Numeric> {
        Box::new(Int2::new(self.value() + value_as::<i16>(other)))
    }

    fn divide(&self, other: &Numeric) -> Box<Float> {
        Box::new(Float4::new(self.value() / value_as::<f32>(other)))
    }
}

impl ValueType for Int2 {
    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; self.size()];
        LittleEndian::write_i16(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), self.is_null(), data)
    }

    fn type_id(&self) -> TypeID {
        TypeID::Int2(self.nullable)
    }

    fn compare(&self, other: &ValueType, comp: Comparator) -> bool {
        match other.type_id() {
            TypeID::Int2() => {
                comp.apply(self.value, cast::<Int2>(other).value())
            }
            TypeID::Int4() => {
                comp.apply(self.value as i32, cast::<Int4>(other).value())
            }
            TypeID::Int8() => {
                comp.apply(self.value as i64, cast::<Int8>(other).value())
            }
            TypeID::Float4() => {
                comp.apply(self.value as f32, cast::<Float4>(other).value())
            }
            TypeID::Float8() => {
                comp.apply(self.value as f64, cast::<Float8>(other).value())
            }
            TypeID::IPv4() => {
                comp.apply(self.value as i64, cast::<IPv4>(other).value() as i64)
            }
            _ => false
        }
    }

    fn is_null(&self) -> bool {
        self.is_null
    }

    fn to_string(&self) -> &str {
        if self.is_null {
            ""
        } else {
            self.value.to_string()
        }
    }
}

#[derive(Clone)]
pub struct Int4 {
    nullable: bool,
    is_null: bool,
    value: i32,
}

impl Int4 {
    // Note that this assumes the type is nullable
    pub fn new(value: i32) -> Self {
        Int4 {
            nullable: true,
            is_null: false,
            value,
        }
    }

    pub fn marshall(nullable: bool, buffer: &BufferType) -> Self {
        Int4 {
            nullable,
            is_null: buffer.is_null(),
            value: LittleEndian::read_i32(buffer.data()),
        }
    }

    pub fn value(&self) -> i32 {
        if self.is_null {
            panic!("Attempting to return i32 value of null Int4");
        }
        self.value
    }
}

impl Integer for Int4 {}

impl Numeric for Int4 {
    fn add(&self, other: &Numeric) -> Box<Numeric> {
        Box::new(Int4::new(self.value() + value_as::<i32>(other)))
    }

    fn divide(&self, other: &Numeric) -> Box<Float> {
        Box::new(Float4::new(self.value() / value_as::<f32>(other)))
    }
}

impl ValueType for Int4 {
    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; self.size()];
        LittleEndian::write_i32(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), self.is_null(), data)
    }

    fn type_id(&self) -> TypeID {
        TypeID::Int4(self.nullable)
    }

    fn compare(&self, other: &ValueType, comp: Comparator) -> bool {
        match other.type_id() {
            TypeID::Int2() => {
                comp.apply(self.value, cast::<Int2>(other).value() as i32)
            }
            TypeID::Int4() => {
                comp.apply(self.value, cast::<Int4>(other).value())
            }
            TypeID::Int8() => {
                comp.apply(self.value as i64, cast::<Int8>(other).value())
            }
            TypeID::Float4() => {
                comp.apply(self.value as f32, cast::<Float4>(other).value())
            }
            TypeID::Float8() => {
                comp.apply(self.value as f64, cast::<Float8>(other).value())
            }
            TypeID::IPv4() => {
                comp.apply(self.value as i64, cast::<IPv4>(other).value() as i64)
            }
            _ => false
        }
    }

    fn is_null(&self) -> bool {
        self.is_null
    }

    fn to_string(&self) -> &str {
        if self.is_null {
            ""
        } else {
            self.value.to_string()
        }
    }
}

#[derive(Clone)]
pub struct Int8 {
    nullable: bool,
    is_null: bool,
    value: i64,
}

impl Integer for Int8 {}

impl Numeric for Int8 {
    fn add(&self, other: &Numeric) -> Box<Numeric> {
        Box::new(Int8::new(self.value() + value_as::<i64>(other)))
    }

    fn divide(&self, other: &Numeric) -> Box<Float> {
        Box::new(Float8::new(self.value() / value_as::<f64>(other)))
    }
}

impl Int8 {
    // Note that this assumes the type is nullable
    pub fn new(value: i64) -> Self {
        Int8 {
            nullable: true,
            is_null: false,
            value,
        }
    }

    pub fn marshall(nullable: bool, buffer: &BufferType) -> Self {
        Int8 {
            nullable,
            is_null: buffer.is_null(),
            value: LittleEndian::read_i64(buffer.data()),
        }
    }

    pub fn value(&self) -> i64 {
        if self.is_null {
            panic!("Attempting to return i64 value of null Int8");
        }
        self.value
    }
}

impl ValueType for Int8 {
    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; self.size()];
        LittleEndian::write_i64(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), self.is_null(), data)
    }

    fn type_id(&self) -> TypeID {
        TypeID::Int8(self.nullable)
    }

    fn compare(&self, other: &ValueType, comp: Comparator) -> bool {
        match other.type_id() {
            TypeID::Int2() => {
                comp.apply(self.value, cast::<Int2>(other).value() as i64)
            }
            TypeID::Int4() => {
                comp.apply(self.value, cast::<Int4>(other).value() as i64)
            }
            TypeID::Int8() => {
                comp.apply(self.value, cast::<Int8>(other).value())
            }
            TypeID::Float4() => {
                comp.apply(self.value as f64, cast::<Float4>(other).value() as f64)
            }
            TypeID::Float8() => {
                comp.apply(self.value as f64, cast::<Float8>(other).value())
            }
            TypeID::IPv4() => {
                comp.apply(self.value, cast::<IPv4>(other).value() as i64)
            }
            _ => false
        }
    }

    fn is_null(&self) -> bool {
        self.is_null
    }

    fn to_string(&self) -> &str {
        if self.is_null {
            ""
        } else {
            self.value.to_string()
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn int2_un_marshall() {
        let int2_value = Int2::new(56);
        let int2_buffer = int2_value.un_marshall();
        assert_eq!(TypeID::Int2, int2_buffer.type_id());

        let data = int2_buffer.data();
        assert_eq!(0x38, data[0]);
        assert_eq!(0x00, data[1]);
    }

    #[test]
    fn int2_type_id() {
        let int2 = Int2::new(56);
        assert_eq!(TypeID::Int2, int2.type_id());
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

        let utf8_string = UTF8String::new("to life, ");
        assert!(!int2.less(&utf8_string));
        assert!(!int2.greater(&utf8_string));
        assert!(!int2.equals(&utf8_string));
    }

    #[test]
    fn int4_un_marshall() {
        let int4_value = Int4::new(2611);
        let int4_buffer = int4_value.un_marshall();
        assert_eq!(TypeID::Int4, int4_buffer.type_id());

        let data = int4_buffer.data();
        assert_eq!(0x33, data[0]);
        assert_eq!(0x0a, data[1]);
        assert_eq!(0x00, data[2]);
    }

    #[test]
    fn int4_type_id() {
        let int4 = Int4::new(2611);
        assert_eq!(TypeID::Int4, int4.type_id());
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

        let utf8_string = UTF8String::new("the universe, ");
        assert!(!int4.less(&utf8_string));
        assert!(!int4.greater(&utf8_string));
        assert!(!int4.equals(&utf8_string));
    }

    #[test]
    fn int8_un_marshall() {
        let int8_value = Int8::new(26119474);
        let int8_buffer = int8_value.un_marshall();
        assert_eq!(TypeID::Int8, int8_buffer.type_id());

        let data = int8_buffer.data();
        assert_eq!(0x32, data[0]);
        assert_eq!(0x8d, data[1]);
        assert_eq!(0x8e, data[2]);
        assert_eq!(0x01, data[3]);
        assert_eq!(0x00, data[4]);
    }

    #[test]
    fn int8_type_id() {
        let int8 = Int8::new(3483646);
        assert_eq!(TypeID::Int8, int8.type_id());
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

        let utf8_string = UTF8String::new("and everything.");
        assert!(!int8.less(&utf8_string));
        assert!(!int8.greater(&utf8_string));
        assert!(!int8.equals(&utf8_string));
    }
}

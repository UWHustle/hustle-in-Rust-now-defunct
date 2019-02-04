extern crate byteorder;

use self::byteorder::{ByteOrder, LittleEndian};

use super::*;

// Define common methods on floating-point types here
trait Float: ValueType {}

pub struct Float4 {
    value: f32
}

impl Float4 {
    pub fn new(value: f32) -> Self {
        Float4 { value }
    }

    pub fn marshall(data: &[u8]) -> Self {
        Float4 { value: LittleEndian::read_f32(&data) }
    }

    pub fn value(&self) -> f32 {
        self.value
    }
}

impl Float for Float4 {}

impl ValueType for Float4 {
    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; self.size()];
        LittleEndian::write_f32(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), data)
    }

    fn type_id(&self) -> TypeID {
        TypeID::Float4
    }

    fn compare(&self, other: &ValueType, comp: Comparator) -> bool {
        match other.type_id() {
            TypeID::Int2 => {
                comp.apply(self.value, cast::<Int2>(other).value() as f32)
            }
            TypeID::Int4 => {
                comp.apply(self.value, cast::<Int4>(other).value() as f32)
            }
            TypeID::Int8 => {
                comp.apply(self.value as f64, cast::<Int8>(other).value() as f64)
            }
            TypeID::Float4 => {
                comp.apply(self.value, cast::<Float4>(other).value())
            }
            TypeID::Float8 => {
                comp.apply(self.value as f64, cast::<Float8>(other).value())
            }
            TypeID::IPv4 => {
                comp.apply(self.value, cast::<IPv4>(other).value() as f32)
            }
            _ => false
        }
    }
}

pub struct Float8 {
    value: f64
}

impl Float8 {
    pub fn new(value: f64) -> Self {
        Float8 { value }
    }

    pub fn marshall(data: &[u8]) -> Self {
        Float8 { value: LittleEndian::read_f64(&data) }
    }

    pub fn value(&self) -> f64 {
        self.value
    }
}

impl Float for Float8 {}

impl ValueType for Float8 {
    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; self.size()];
        LittleEndian::write_f64(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), data)
    }

    fn type_id(&self) -> TypeID {
        TypeID::Float8
    }

    fn compare(&self, other: &ValueType, comp: Comparator) -> bool {
        match other.type_id() {
            TypeID::Int2 => {
                comp.apply(self.value, cast::<Int2>(other).value() as f64)
            }
            TypeID::Int4 => {
                comp.apply(self.value, cast::<Int4>(other).value() as f64)
            }
            TypeID::Int8 => {
                comp.apply(self.value, cast::<Int8>(other).value() as f64)
            }
            TypeID::Float4 => {
                comp.apply(self.value, cast::<Float4>(other).value() as f64)
            }
            TypeID::Float8 => {
                comp.apply(self.value, cast::<Float8>(other).value())
            }
            TypeID::IPv4 => {
                comp.apply(self.value, cast::<IPv4>(other).value() as f64)
            }
            _ => false
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn float4_un_marshall() {
        let float4_value = Float4::new(13.7);
        let float4_buffer = float4_value.un_marshall();
        assert_eq!(TypeID::Float4, float4_buffer.type_id());

        let data = float4_buffer.data();
        assert_eq!(0x41, data[0]);
        assert_eq!(0x5b, data[1]);
        assert_eq!(0x33, data[2]);
        assert_eq!(0x33, data[3]);
    }

    #[test]
    fn float4_type_id() {
        let float4 = Float4::new(13.7);
        assert_eq!(TypeID::Float4, float4.type_id());
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
    fn float8_un_marshall() {
        let float8_value = Float8::new(12228.444);
        let float8_buffer = float8_value.un_marshall();
        assert_eq!(TypeID::Float8, float8_buffer.type_id());

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
    fn float8_type_id() {
        let float8 = Float8::new(12228.444);
        assert_eq!(TypeID::Float8, float8.type_id());
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
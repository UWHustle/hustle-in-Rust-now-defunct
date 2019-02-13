extern crate byteorder;

use self::byteorder::{ByteOrder, LittleEndian};

use super::*;

/// Contains methods common to IP address types
trait IPAddress: Value {}

/* ============================================================================================== */

/// An IPv4 address type
#[derive(Clone, Debug)]
pub struct IPv4 {
    nullable: bool,
    is_null: bool,
    value: u32,
}

impl IPv4 {
    pub fn new(value: u32) -> Self {
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
        Self::new(string.parse::<u32>().expect("Parsing failed"))
    }

    pub fn marshall(nullable: bool, is_null: bool, data: &[u8]) -> Self {
        Self {
            nullable,
            is_null,
            value: LittleEndian::read_u32(data),
        }
    }

    pub fn value(&self) -> u32 {
        if self.is_null {
            panic!(null_value(self.type_id()));
        }
        self.value
    }
}

impl IPAddress for IPv4 {}

impl Value for IPv4 {
    fn type_id(&self) -> TypeID {
        TypeID::new(Variant::IPv4, self.nullable)
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
        LittleEndian::write_u32(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), self.is_null(), data)
    }

    fn compare(&self, other: &Value, comp: Comparator) -> bool {
        match other.type_id().variant {
            Variant::Int2 => {
                comp.apply(self.value as i64, cast_value::<Int2>(other).value() as i64)
            }
            Variant::Int4 => {
                comp.apply(self.value as i64, cast_value::<Int4>(other).value() as i64)
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
                comp.apply(self.value, cast_value::<IPv4>(other).value())
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
    fn ipv4_type_id() {
        let ipv4 = IPv4::new(88997);
        assert_eq!(TypeID::new(Variant::IPv4, true), ipv4.type_id());
    }

    #[test]
    fn ipv4_un_marshall() {
        let ipv4_value = IPv4::new(88997);
        let ipv4_buffer = ipv4_value.un_marshall();
        assert_eq!(TypeID::new(Variant::IPv4, true), ipv4_buffer.type_id());

        let data = ipv4_buffer.data();
        assert_eq!(0xa5, data[0]);
        assert_eq!(0x5b, data[1]);
        assert_eq!(0x01, data[2]);
        assert_eq!(0x00, data[3]);
    }

    #[test]
    fn ipv4_compare() {
        let ipv4 = IPv4::new(2105834626);

        let int2 = Int2::new(120);
        assert!(!ipv4.less(&int2));
        assert!(ipv4.greater(&int2));
        assert!(!ipv4.equals(&int2));

        let int4 = Int4::new(1344);
        assert!(!ipv4.less(&int4));
        assert!(ipv4.greater(&int4));
        assert!(!ipv4.equals(&int4));

        let int8 = Int4::new(2105834626);
        assert!(!ipv4.less(&int8));
        assert!(!ipv4.greater(&int8));
        assert!(ipv4.equals(&int8));

        let float4 = Float4::new(-1234.5);
        assert!(!ipv4.less(&float4));
        assert!(ipv4.greater(&float4));
        assert!(!ipv4.equals(&float4));

        let float8 = Float8::new(2105834626.0);
        assert!(!ipv4.less(&float8));
        assert!(!ipv4.greater(&float8));
        assert!(ipv4.equals(&float8));

        let utf8_string = UTF8String::new("localhost");
        assert!(!ipv4.less(&utf8_string));
        assert!(!ipv4.greater(&utf8_string));
        assert!(!ipv4.equals(&utf8_string));
    }
}
extern crate byteorder;

use self::byteorder::{ByteOrder, LittleEndian};

use super::*;

// Define common methods on ip-address types here
trait IPAddress: ValueType {}

#[derive(Clone)]
pub struct IPv4 {
    nullable: bool,
    is_null: bool,
    value: u32,
}

impl IPv4 {
    // Note that this assumes the type is nullable
    pub fn new(value: u32) -> Self {
        IPv4 {
            nullable: true,
            is_null: false,
            value,
        }
    }

    pub fn marshall(nullable: bool, buffer: &BufferType) -> Self {
        IPv4 {
            nullable,
            is_null: buffer.is_null(),
            value: LittleEndian::read_u32(buffer.data()),
        }
    }

    pub fn value(&self) -> u32 {
        if self.is_null {
            panic!("Attempting to retrieve u32 value of null IPv4");
        }
        self.value
    }
}

impl IPAddress for IPv4 {}

impl ValueType for IPv4 {
    fn un_marshall(&self) -> OwnedBuffer {
        let mut data: Vec<u8> = vec![0; self.size()];
        LittleEndian::write_u32(&mut data, self.value);
        OwnedBuffer::new(self.type_id(), self.is_null(), data)
    }

    fn type_id(&self) -> TypeID {
        TypeID::IPv4(self.nullable)
    }

    fn compare(&self, other: &ValueType, comp: Comparator) -> bool {
        match other.type_id() {
            TypeID::Int2() => {
                comp.apply(self.value as i64, cast::<Int2>(other).value() as i64)
            }
            TypeID::Int4() => {
                comp.apply(self.value as i64, cast::<Int4>(other).value() as i64)
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
                comp.apply(self.value, cast::<IPv4>(other).value())
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
    fn ipv4_un_marshall() {
        let ipv4_value = IPv4::new(88997);
        let ipv4_buffer = ipv4_value.un_marshall();
        assert_eq!(TypeID::IPv4, ipv4_buffer.type_id());

        let data = ipv4_buffer.data();
        assert_eq!(0xa5, data[0]);
        assert_eq!(0x5b, data[1]);
        assert_eq!(0x01, data[2]);
        assert_eq!(0x00, data[3]);
    }

    #[test]
    fn ipv4_type_id() {
        let ipv4 = IPv4::new(88997);
        assert_eq!(TypeID::IPv4, ipv4.type_id());
    }

    #[test]
    fn ipv4_compare() {
        let ipv4 = IPv4::new(2105834626);

        let int2 = Int2::new(120);
        assert!(!ipv4.less(&int2));
        assert!(ipv4.greater(&int2));
        assert!(!ipv4.equals(&int2));

        let int4 = Int4::new(1344);
        assert!(!ipv4.less(&int2));
        assert!(ipv4.greater(&int2));
        assert!(!ipv4.equals(&int2));

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
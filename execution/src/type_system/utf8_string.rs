extern crate byteorder;

use self::byteorder::{ByteOrder, LittleEndian};

use super::*;

/// A variable-length UTF8-encoded string
#[derive(Clone, Debug)]
pub struct UTF8String {
    value: Box<String>,
    nullable: bool,
    is_null: bool,
}

impl UTF8String {
    pub fn new(value: &str, nullable: bool) -> Self {
        let string = value.to_string();
        Self {
            value: Box::new(string),
            nullable,
            is_null: false,
        }
    }

    pub fn from(value: &str) -> Self {
        Self::new(value, true)
    }

    pub fn create_null() -> Self {
        Self {
            value: Box::new(String::new()),
            nullable: true,
            is_null: true,
        }
    }

    pub fn marshall(data: &[u8], nullable: bool, is_null: bool) -> Self {
        let mut vec_data: Vec<u8> = vec!();
        vec_data.clone_from_slice(data);
        let value = String::from_utf8(vec_data).expect("Invalid UTF8 string");
        Self {
            value: Box::new(value),
            nullable,
            is_null,
        }
    }

    pub fn next_size(data: &[u8]) -> usize {
        LittleEndian::read_u64(&data[..8]) as usize
    }

    pub fn value(&self) -> &str {
        if self.is_null {
            panic!("Attempting to return string slice of null UTF8String");
        }
        &self.value
    }
}

impl Value for UTF8String {
    fn type_id(&self) -> TypeID {
        TypeID::new(Variant::UTF8String, self.nullable)
    }

    fn is_null(&self) -> bool {
        self.is_null
    }

    fn to_string(&self) -> String {
        if self.is_null {
            String::new()
        } else {
            self.value().to_string()
        }
    }

    fn un_marshall(&self) -> OwnedBuffer {
        let mut value: Vec<u8> = vec![0; self.size()];
        value.clone_from_slice(self.value.as_bytes());
        OwnedBuffer::new(value, self.type_id(), self.is_null())
    }

    fn compare(&self, other: &Value, comp: Comparator) -> bool {
        match other.type_id().variant {
            Variant::UTF8String => {
                comp.apply(&self.value, &Box::new(cast_value::<UTF8String>(other).value().to_string()))
            }
            _ => {
                panic!(incomparable(self.type_id(), other.type_id()));
            }
        }
    }

    /// Returns the size of the string (overrides the default implementation)
    fn size(&self) -> usize {
        self.value.len()
    }
}

/* ============================================================================================== */

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn utf8_string_un_marshall() {
        let utf8_string_value = UTF8String::from("Hello!");
        let utf8_string_buffer = utf8_string_value.un_marshall();
        assert_eq!(TypeID::new(Variant::UTF8String, true), utf8_string_buffer.type_id());

        let data = utf8_string_buffer.data();
        assert_eq!('H' as u8, data[0]);
        assert_eq!('e' as u8, data[1]);
        assert_eq!('l' as u8, data[2]);
        assert_eq!('l' as u8, data[3]);
        assert_eq!('o' as u8, data[4]);
        assert_eq!('!' as u8, data[5]);
    }

    #[test]
    fn utf8_string_size() {
        let utf8_string = UTF8String::from("Do no evil");
        assert_eq!(10, utf8_string.size());
    }

    #[test]
    fn utf8_string_type_id() {
        let utf8_string = UTF8String::from("Chocolate donuts");
        assert_eq!(TypeID::new(Variant::UTF8String, true), utf8_string.type_id());
    }

    #[test]
    fn utf8_string_compare() {
        let alphabet = UTF8String::from("alphabet");

        let aardvark = UTF8String::from("aardvark");
        assert!(!alphabet.less(&aardvark));
        assert!(alphabet.greater(&aardvark));
        assert!(!alphabet.equals(&aardvark));

        let elephant = UTF8String::from("elephant");
        assert!(alphabet.less(&elephant));
        assert!(!alphabet.greater(&elephant));
        assert!(!alphabet.equals(&elephant));
    }

    #[test]
    #[should_panic]
    fn invalid_utf8_string_compare() {
        let alphabet = UTF8String::from("alphabet");
        let int4 = Int4::from(1234);
        int4.equals(&alphabet);
    }
}

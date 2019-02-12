use super::*;

#[derive(Clone, Debug)]
pub struct UTF8String {
    nullable: bool,
    is_null: bool,
    value: Box<String>,
}

impl UTF8String {
    // Note that this assumes the type is nullable
    pub fn new(value: &str) -> Self {
        let string = value.to_string();
        UTF8String {
            nullable: true,
            is_null: false,
            value: Box::new(string),
        }
    }

    pub fn create_null() -> Self {
        UTF8String {
            nullable: true,
            is_null: true,
            value: Box::new("".to_string()),
        }
    }

    pub fn marshall(nullable: bool, is_null: bool, data: &[u8]) -> Self {
        let mut vec_data: Vec<u8> = vec!();
        vec_data.clone_from_slice(data);
        let value = String::from_utf8(vec_data).expect("Invalid UTF8 string");
        UTF8String {
            nullable,
            is_null,
            value: Box::new(value),
        }
    }

    pub fn value(&self) -> &str {
        if self.is_null {
            panic!("Attempting to return string slice of null UTF8String");
        }
        &self.value
    }
}

impl Value for UTF8String {
    fn un_marshall(&self) -> OwnedBuffer {
        let mut value: Vec<u8> = vec!();
        value.clone_from_slice(self.value.as_bytes());
        OwnedBuffer::new(self.type_id(), self.is_null(), value)
    }

    // Overrides the default implementation
    fn size(&self) -> usize {
        self.value.len()
    }

    fn type_id(&self) -> TypeID {
        TypeID::new(Variant::UTF8String, self.nullable)
    }

    fn compare(&self, other: &Value, comp: Comparator) -> bool {
        match other.type_id().variant {
            Variant::UTF8String => {
                comp.apply(&self.value, &Box::new(cast_value::<UTF8String>(other).value().to_string()))
            }
            _ => false
        }
    }

    fn is_null(&self) -> bool {
        self.is_null
    }

    fn to_string(&self) -> String {
        if self.is_null {
            String::from("")
        } else {
            self.value().to_string()
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn utf8_string_un_marshall() {
        let utf8_string_value = UTF8String::new("Hello!");
        let utf8_string_buffer = utf8_string_value.un_marshall();
        assert_eq!(TypeID::new(Variant::UTF8String, true), utf8_string_buffer.type_id());

        let data = utf8_string_buffer.data();
        assert_eq!('H' as u8, data[0]);
        assert_eq!('e' as u8, data[1]);
        assert_eq!('l' as u8, data[2]);
        assert_eq!('l' as u8, data[3]);
        assert_eq!('l' as u8, data[4]);
        assert_eq!('o' as u8, data[5]);
        assert_eq!('!' as u8, data[6]);
    }

    #[test]
    fn utf8_string_size() {
        let utf8_string = UTF8String::new("Do no evil");
        assert_eq!(10, utf8_string.size());
    }

    #[test]
    fn utf8_string_type_id() {
        let utf8_string = UTF8String::new("Chocolate donuts");
        assert_eq!(TypeID::new(Variant::UTF8String, true), utf8_string.type_id());
    }

    #[test]
    fn utf8_string_compare() {
        let alphabet = UTF8String::new("alphabet");

        let aardvark = UTF8String::new("aardvark");
        assert!(!alphabet.less(&aardvark));
        assert!(alphabet.greater(&aardvark));
        assert!(!alphabet.equals(&aardvark));

        let elephant = UTF8String::new("elephant");
        assert!(alphabet.less(&elephant));
        assert!(!alphabet.greater(&elephant));
        assert!(!alphabet.equals(&elephant));

        let int4 = Int4::new(1234);
        assert!(!alphabet.less(&int4));
        assert!(!alphabet.greater(&int4));
        assert!(!alphabet.equals(&int4));
    }
}

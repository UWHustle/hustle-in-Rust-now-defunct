use super::*;

pub struct UTF8String {
    value: Box<String>
}

impl UTF8String {
    pub fn new(value: &str) -> Self {
        let string = value.to_string();
        UTF8String { value: Box::new(string) }
    }

    pub fn marshall(data: &[u8]) -> Self {
        let mut vec_data: Vec<u8> = vec!();
        vec_data.clone_from_slice(data);
        let value = String::from_utf8(vec_data).expect("Invalid UTF8 string");
        UTF8String { value: Box::new(value) }
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

impl ValueType for UTF8String {
    fn un_marshall(&self) -> OwnedBuffer {
        let mut value: Vec<u8> = vec!();
        value.clone_from_slice(self.value.as_bytes());
        OwnedBuffer::new(self.type_id(), value)
    }

    // Overrides the default implementation
    fn size(&self) -> usize {
        self.value.len()
    }

    fn type_id(&self) -> TypeID {
        TypeID::UTF8String
    }

    fn compare(&self, other: &ValueType, comp: Comparator) -> bool {
        match other.type_id() {
            TypeID::UTF8String() => {
                comp.apply(&self.value, &Box::new(cast::<UTF8String>(other).value().to_string()))
            }
            _ => false
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
        assert_eq!(TypeID::UTF8String, utf8_string_buffer.type_id());

        let data = utf8_string_buffer.data();
        assert_eq!('H', data[0]);
        assert_eq!('e', data[1]);
        assert_eq!('l', data[2]);
        assert_eq!('l', data[3]);
        assert_eq!('l', data[4]);
        assert_eq!('o', data[5]);
        assert_eq!('!', data[6]);
    }

    #[test]
    fn utf8_string_size() {
        let uf8_string = UTF8String::new("Do no evil");
        assert_eq!(10, utf8_string.size());
    }

    #[test]
    fn utf8_string_type_id() {
        let utf8_string = UTF8String::new("Chocolate donuts");
        assert_eq!(TypeID::UTF8String, utf8_string.type_id());
    }

    #[test]
    fn utf8_string_compare() {
        let alphabet = UTF8String::new("alphabet");

        let aardvark = UTF8String::new("aardvark");
        assert!(!alphabet.less(&aardvark));
        assert!(alphabet.greater(&aardvark));
        assert!(!alphabet.equals(aardvark));

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

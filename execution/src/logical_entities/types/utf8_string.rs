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
            TypeID::UTF8String => {
                comp.apply(&self.value, &Box::new(cast::<UTF8String>(other).value().to_string()))
            }
            _ => false
        }
    }
}

#[cfg(test)]
mod test {
    // TODO: Place unit tests here

    #[test]
    fn utf8_string_un_marshall() {

    }

    #[test]
    fn utf8_string_type_id() {

    }

}

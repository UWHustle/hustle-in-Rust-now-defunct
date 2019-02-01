use super::*;

pub struct UTF8String {
    value: Box<String>
}

impl UTF8String {
    pub fn new(data: &[u8]) -> Self {
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

    fn size(&self) -> usize { self.value.len() }

    fn type_id(&self) -> TypeID {
        TypeID::UTF8String
    }

    fn equals(&self, other: &ValueType) -> bool {
        match other.type_id() {
            TypeID::UTF8String => {
                self.value.eq(&Box::new(cast::<UTF8String>(other).value().to_string()))
            }
            _ => false
        }
    }

    fn less_than(&self, other: &ValueType) -> bool {
        match other.type_id() {
            TypeID::UTF8String => {
                self.value.lt(&Box::new(cast::<UTF8String>(other).value().to_string()))
            }
            _ => false
        }
    }

    fn greater_than(&self, other: &ValueType) -> bool {
        match other.type_id() {
            TypeID::UTF8String => {
                self.value.gt(&Box::new(cast::<UTF8String>(other).value().to_string()))
            }
            _ => false
        }
    }
}

#[cfg(test)]
mod test {
    // TODO: Place unit tests here
}

use logical_entities::types::DataType;
use std::fmt;

#[derive(Clone, Debug, PartialEq)]
pub struct Value {
    value: Vec<u8>,
    datatype: DataType,
}

impl Value {
    pub fn new(datatype:DataType, value: Vec<u8>) -> Self {
        Value {
            datatype, value
        }
    }

    pub fn get_size(&self) -> usize {
        self.datatype.get_next_length(self.value.as_slice())
    }

    pub fn format(datatype: DataType, value: Vec<u8>) -> String {
        format!("{}", datatype.to_string(&value))
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.datatype.to_string(&self.value))
    }
}


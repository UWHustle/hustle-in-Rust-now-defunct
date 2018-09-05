use logical_entities::types::DataType;

#[derive(Clone, Debug, PartialEq)]
pub struct Value {
    value: Vec<u8>,
    datatype: DataType,
}
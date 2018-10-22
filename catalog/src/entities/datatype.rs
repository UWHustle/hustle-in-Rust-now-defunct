#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DataType {
    pub name: String,
}

impl DataType {
    pub fn new(name: String) -> DataType {
        DataType {
            name: name
        }
    }
}
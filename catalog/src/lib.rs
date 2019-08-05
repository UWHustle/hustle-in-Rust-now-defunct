#[macro_use]
extern crate serde;

use hustle_types::Type;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub block_ids: Vec<u64>,
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>, block_ids: Vec<u64>) -> Self {
        Table {
            name,
            columns,
            block_ids,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub column_type: Type,
}

use logical_entities::schema::Schema;
use type_system::*;

#[derive(Clone)]
pub struct Row {
    schema: Schema,
    values: Vec<Box<Value>>,
}

impl Row {
    pub fn new(schema: Schema, values: Vec<Box<Value>>) -> Self {
        Row { schema, values }
    }

    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    pub fn get_values(&self) -> &Vec<Box<Value>> {
        &self.values
    }

    pub fn get_size(&self) -> usize {
        let mut total_size = 0;
        for value in &self.values {
            total_size += value.size();
        }
        total_size
    }

    pub fn to_string(&self) -> String {
        let mut string_list = vec![];
        for value in &self.values {
            string_list.push(value.to_string());
        }
        string_list.join(", ")
    }
}

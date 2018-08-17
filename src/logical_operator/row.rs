use logical_operator::schema::Schema;

#[derive(Clone, Debug)]
pub struct Row {
    schema: Schema,
    values: Vec<u64>,
}

impl Row {
    pub fn new(schema: Schema, values: Vec<u64>) -> Self {
        Row {
            schema, values
        }
    }

    pub fn get_schema(&self) -> &Schema {
        return &self.schema;
    }

    pub fn get_values(&self) -> &Vec<u64> {
        return &self.values;
    }
}

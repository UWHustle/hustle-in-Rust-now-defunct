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
}

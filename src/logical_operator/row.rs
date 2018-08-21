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


use logical_operator::schema::cSchema;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct cRow {
    values: [u64; 2 ],
    schema: cSchema,
}

impl cRow {
    pub fn to_row(&self) -> Row {
        let values = self.values.iter().filter(|_value|{true}).map(|value|{*value as u64}).collect::<Vec<_>>();
        let schema = self.schema.to_schema();

        Row {
            schema,values
        }
    }
}
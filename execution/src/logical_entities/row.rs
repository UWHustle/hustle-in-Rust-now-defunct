use logical_entities::schema::Schema;
use logical_entities::value::Value;

#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    schema: Schema,
    values: Vec<Value>,
}

impl Row {
    pub fn new(schema: Schema, values: Vec<Value>) -> Self {
        Row {
            schema, values
        }
    }

    pub fn get_schema(&self) -> &Schema {
        return &self.schema;
    }
    pub fn get_values(&self) -> &Vec<Value> {
        return &self.values;
    }
    pub fn get_size(&self) -> usize {
        let mut total_size = 0;
        for value in &self.values {
            total_size += value.get_size();
        }
        total_size
    }
}

/*
use logical_entities::schema::ExtSchema;


#[repr(C)]
#[derive(Clone, PartialEq)]
pub struct ExtRow {
    values: [u64; 2 ],
    schema: ExtSchema,
}

impl ExtRow {
    pub fn to_row(&self) -> Row {
        let values = self.values.iter().filter(|_value|{true}).map(|value|{*value as u64}).collect::<Vec<_>>();
        let schema = self.schema.to_schema();

        Row {
            schema,values
        }
    }

    pub fn from_row(row: Row) -> ExtRow {
        let values = [1,2];
        let schema = ExtSchema::from_schema(row.get_schema().clone());

        ExtRow {
            values,schema
        }
    }
}*/
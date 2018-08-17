use logical_operator::schema::Schema;
use logical_operator::column::Column;

#[derive(Clone, Debug)]
pub struct LogicalRelation {
    name: String,
    schema: Schema,
}

impl LogicalRelation {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        let schema = Schema::new(columns);
        LogicalRelation {
            name, schema
        }
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn get_columns(&self) -> &Vec<Column> {
        &self.schema.get_columns()
    }

    pub fn get_schema(&self) -> &Schema {
        return &self.schema;
    }

    pub fn get_filename(&self) -> String {
        format!("{}{}",self.get_name(),".hsl")
    }

    pub fn get_row_size(&self) -> usize {return self.schema.get_row_size();}

    pub fn get_total_size(&self) -> usize {
        use std::fs;
        let mut total_size = fs::metadata(self.get_filename()).unwrap().len() as usize;
        total_size = total_size/self.get_row_size();
        total_size = (total_size+1) * self.get_row_size();

        return total_size;
    }
}

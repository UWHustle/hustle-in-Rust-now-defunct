const ROW_COUNT: usize = 1024*16;

use logical_operator::column::Column;

#[derive(Clone, Debug)]
pub struct LogicalRelation {
    name: String,
    columns: Vec<Column>,
}

impl LogicalRelation {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        LogicalRelation {
            name, columns
        }
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn to_filename(&self) -> String {
        format!("{}{}",self.get_name(),".hsl")
    }

    pub fn get_row_size(&self) -> usize {return self.columns.iter().map(|s| s.get_size()).sum::<usize>();}

    pub fn get_row_count(&self) -> usize {
        return ROW_COUNT;
    }

    pub fn get_total_size(&self) -> usize {
        return self.get_row_count() * self.get_row_size();
    }
}

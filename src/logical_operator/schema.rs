use logical_operator::column::Column;

#[derive(Clone, Debug)]
pub struct Schema {
    columns: Vec<Column>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Schema {
            columns
        }
    }

    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn get_row_size(&self) -> usize {return self.columns.iter().map(|s| s.get_size()).sum::<usize>();}
}

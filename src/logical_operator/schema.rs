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

use logical_operator::column::cColumn;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct cSchema {
    columns: [cColumn; 2],
}

impl cSchema {
    pub fn to_schema(&self) -> Schema {
        let columns = self.columns.iter().filter(|column:&&cColumn|{column.is_valid()}).map(|column:&cColumn|{column.to_column()}).collect::<Vec<_>>();
        Schema::new(columns)
    }
}
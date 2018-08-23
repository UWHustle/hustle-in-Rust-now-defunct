use logical_entities::column::Column;

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

use logical_entities::column::ExtColumn;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExtSchema {
    columns: [ExtColumn; 2],
}

impl ExtSchema {
    pub fn to_schema(&self) -> Schema {
        let columns = self.columns.iter().filter(|column:&&ExtColumn|{column.is_valid()}).map(|column:&ExtColumn|{column.to_column()}).collect::<Vec<_>>();
        Schema::new(columns)
    }

    pub fn from_schema(schema: Schema) -> ExtSchema {
        let ext_columns:Vec<ExtColumn> = schema.columns.iter().map(|column:&Column| { ExtColumn::from_column(column.clone())}).collect();

        let columns = [ext_columns[0], ext_columns[1]];

        ExtSchema {
            columns
        }
    }
}
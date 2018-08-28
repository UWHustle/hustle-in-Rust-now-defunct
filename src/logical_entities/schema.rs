use logical_entities::column::Column;

#[derive(Clone, Debug, PartialEq)]
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
#[derive(Clone, Debug, PartialEq)]
pub struct ExtSchema {
    columns: [ExtColumn; 2],
}

impl ExtSchema {
    pub fn to_schema(&self) -> Schema {
        let columns = self.columns.iter().filter(|column:&&ExtColumn|{
            column.is_valid()
        }).map(|column:&ExtColumn|{
            column.to_column()}).collect::<Vec<_>>();
        Schema::new(columns)
    }

    pub fn from_schema(schema: Schema) -> ExtSchema {
        let ext_columns:Vec<ExtColumn> = schema.columns.iter().map(|column:&Column| {
            ExtColumn::from_column(column.clone())
        }).collect();

        let columns = [ext_columns.first().unwrap().clone(), ext_columns.last().clone().unwrap().clone()];

        ExtSchema {
            columns
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn schema_create() {
        use logical_entities::schema::Schema;
        use logical_entities::column::Column;

        let schema = Schema::new(vec!(
            Column::new("a".to_string(), 8),
            Column::new("b".to_string(), 8))
        );

        assert_eq!(schema.get_columns().first().unwrap().get_name(), &"a".to_string());
        assert_eq!(schema.get_columns().last().unwrap().get_name(), &"b".to_string());

        assert_eq!(schema.get_row_size(), 16);
    }

    #[test]
    fn ext_schema_create() {
        use logical_entities::schema::Schema;
        use logical_entities::schema::ExtSchema;
        use logical_entities::column::Column;

        let schema = Schema::new(vec!(
            Column::new("a".to_string(), 8),
            Column::new("b".to_string(), 8))
        );

        let ext_schema = ExtSchema::from_schema(schema.clone());

        let r_schema = ext_schema.to_schema();

        //assert_eq!(r_schema, schema);
        //TODO: Fix this test.  ExtColumn::from_column doesn't seem to work in from_schema.
    }
}
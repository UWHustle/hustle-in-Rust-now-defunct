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

#[cfg(test)]
mod tests {
    #[test]
    fn schema_create() {
        use logical_entities::schema::Schema;
        use logical_entities::column::Column;

        let schema = Schema::new(vec!(
            Column::new("a".to_string(), "Int".to_string()),
            Column::new("b".to_string(), "Int".to_string()))
        );

        assert_eq!(schema.get_columns().first().unwrap().get_name(), &"a".to_string());
        assert_eq!(schema.get_columns().last().unwrap().get_name(), &"b".to_string());

        assert_eq!(schema.get_row_size(), 16);
    }

}

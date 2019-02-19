use logical_entities::column::Column;

#[derive(Clone, Debug, PartialEq)]
pub struct Schema {
    columns: Vec<Column>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Schema { columns }
    }

    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn get_row_size(&self) -> usize {
        self.columns.iter().map(|s| s.get_size()).sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use logical_entities::column::Column;
    use logical_entities::schema::Schema;
    use type_system::type_id::*;

    #[test]
    fn schema_create() {
        let a = Column::new("a".to_string(), TypeID::new(Variant::Int4, true));
        let b = Column::new("b".to_string(), TypeID::new(Variant::Int4, true));
        let schema = Schema::new(vec![a.clone(), b.clone()]);

        assert_eq!(
            schema.get_columns().first().unwrap().get_name(),
            &"a".to_string()
        );
        assert_eq!(
            schema.get_columns().last().unwrap().get_name(),
            &"b".to_string()
        );
        assert_eq!(schema.get_row_size(), a.get_size() + b.get_size());
    }
}

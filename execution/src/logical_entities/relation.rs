use logical_entities::column::Column;
use logical_entities::schema::Schema;

use storage::StorageManager;

#[derive(Clone, Debug, PartialEq)]
pub struct Relation {
    name: String,
    schema: Schema,
}

impl Relation {
    pub fn new(name: &str, schema: Schema) -> Self {
        Self {
            name: String::from(name),
            schema,
        }
    }

    pub fn null() -> Self {
        let name = "Null".to_string();
        let schema = Schema::new(vec![]);
        Self { name, schema }
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn get_columns(&self) -> &Vec<Column> {
        &self.schema.get_columns()
    }

    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    pub fn get_filename(&self) -> String {
        format!("test-data/{}{}", self.get_name(), ".hsl")
    }

    pub fn get_row_size(&self) -> usize {
        self.schema.get_row_size()
    }

    pub fn get_total_size(&self, storage_manager: &StorageManager) -> usize {
        match storage_manager.get(self.get_name()) {
            Some(value) => value.len(),
            None => 0,
        }
    }

    pub fn column_from_name(&self, name: &str) -> Result<Column, String> {
        for column in self.get_columns() {
            if name == column.get_name() {
                return Ok(column.clone());
            }
        }
        Err(format!("column {} not found", name))
    }

    pub fn columns_from_names(&self, col_names: Vec<&str>) -> Result<Vec<Column>, String> {
        let mut columns: Vec<Column> = vec![];
        for name in col_names {
            columns.push(self.column_from_name(name)?);
        }
        Ok(columns)
    }
}

#[cfg(test)]
mod tests {
    use logical_entities::column::Column;
    use logical_entities::relation::Relation;
    use logical_entities::schema::Schema;
    use type_system::data_type::*;

    #[test]
    fn relation_create() {
        let a = Column::new("a".to_string(), DataType::new(Variant::Int4, true));
        let b = Column::new("b".to_string(), DataType::new(Variant::Int4, true));
        let schema = Schema::new(vec![a.clone(), b.clone()]);
        let relation = Relation::new("Test", schema.clone());

        assert_eq!(relation.get_name(), &"Test".to_string());
        assert_eq!(
            relation.get_columns().first().unwrap().get_name(),
            &"a".to_string()
        );
        assert_eq!(
            relation.get_columns().last().unwrap().get_name(),
            &"b".to_string()
        );
        assert_eq!(relation.get_schema(), &schema);
        assert_eq!(relation.get_filename(), "test-data/Test.hsl".to_string());
        assert_eq!(relation.get_row_size(), a.get_size() + b.get_size());
    }
}

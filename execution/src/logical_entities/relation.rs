use logical_entities::schema::Schema;
use logical_entities::column::Column;

#[derive(Clone, Debug, PartialEq)]
pub struct Relation {
    name: String,
    schema: Schema,
}

impl Relation {
    pub fn new(name: String, schema: Schema) -> Self {
        Self {
            name,
            schema,
        }
    }

    pub fn null() -> Self {
        let name = "Null".to_string();
        let schema = Schema::new(vec!());
        Self {
            name,
            schema,
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
        format!("test-data/{}{}", self.get_name(), ".hsl")
    }

    pub fn get_row_size(&self) -> usize { return self.schema.get_row_size(); }

    pub fn get_total_size(&self) -> usize {
        use std::fs;

        match fs::metadata(self.get_filename()) {
            Ok(n) => {
                let mut total_size = n.len() as usize;
                total_size = total_size / self.get_row_size();
                total_size = (total_size) * self.get_row_size();
                return total_size;
            }
            Err(err) => {
                println!("Error getting file size: {}", err);
                return 0;
            }
        }
    }

    pub fn get_n_rows(&self) -> usize {
        self.get_total_size() / self.get_row_size()
    }
}

#[cfg(test)]
mod tests {
    use logical_entities::column::Column;
    use logical_entities::relation::Relation;
    use logical_entities::schema::Schema;
    use type_system::type_id::*;

    #[test]
    fn relation_create() {
        let a = Column::new("a".to_string(), TypeID::new(Variant::Int4, true));
        let b = Column::new("b".to_string(), TypeID::new(Variant::Int4, true));
        let schema = Schema::new(vec!(a.clone(), b.clone()));
        let relation = Relation::new("Test".to_string(), schema.clone());

        assert_eq!(relation.get_name(), &"Test".to_string());
        assert_eq!(relation.get_columns().first().unwrap().get_name(), &"a".to_string());
        assert_eq!(relation.get_columns().last().unwrap().get_name(), &"b".to_string());
        assert_eq!(relation.get_schema(), &schema);
        assert_eq!(relation.get_filename(), "test-data/Test.hsl".to_string());
        assert_eq!(relation.get_row_size(), a.get_size() + b.get_size());
    }
}

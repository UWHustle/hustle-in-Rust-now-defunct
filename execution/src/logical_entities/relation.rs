use logical_entities::schema::Schema;
use logical_entities::column::Column;

#[derive(Clone, Debug, PartialEq)]
pub struct Relation {
    name: String,
    schema: Schema,
}

impl Relation {
    pub fn new(name: String, schema: Schema) -> Self {
        Relation {
            name, schema
        }
    }

    pub fn null()-> Self {
        let name = "Null".to_string();
        let schema = Schema::new(vec!());
        Relation{
            name,schema
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
        format!("test-data/{}{}",self.get_name(),".hsl")
    }

    pub fn get_row_size(&self) -> usize {return self.schema.get_row_size();}

    pub fn get_total_size(&self) -> usize {
        use std::fs;

        match fs::metadata(self.get_filename()) {
            Ok(n) => {
                let mut total_size = n.len() as usize;
                total_size = total_size/self.get_row_size();
                total_size = (total_size) * self.get_row_size();
                return total_size
            },
            Err(err) => {
                println!("Error getting file size: {}",err);
                return 0;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn relation_create() {
        use logical_entities::relation::Relation;
        use logical_entities::schema::Schema;
        use logical_entities::column::Column;

        let relation = Relation::new("Test".to_string(),
                                   Schema::new(vec!(
                                                Column::new("a".to_string(),"Int".to_string()),
                                                Column::new("b".to_string(), "Int".to_string()))
                                   ));

        assert_eq!(relation.get_name(),&"Test".to_string());

        assert_eq!(relation.get_columns().first().unwrap().get_name(),&"a".to_string());
        assert_eq!(relation.get_columns().last().unwrap().get_name(),&"b".to_string());

        assert_eq!(relation.get_schema(),&Schema::new(vec!(
            Column::new("a".to_string(),"Int".to_string()),
            Column::new("b".to_string(), "Int".to_string()))));

        assert_eq!(relation.get_filename(),"test-data/Test.hsl".to_string());

        assert_eq!(relation.get_row_size(), 16);

        //assert_eq!(relation.get_total_size(), 0);
        //TODO: Support this in a unit test, not just integrated test
    }
}

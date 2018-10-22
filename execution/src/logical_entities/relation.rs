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

use std::os::raw::c_char;
use logical_entities::schema::ExtSchema;

#[repr(C)]
#[derive(Clone, PartialEq)]
pub struct ExtRelation {
    name: *const c_char,
    schema: ExtSchema,
}

impl ExtRelation {
    pub fn to_relation(&self) -> Relation {
        let c_name = self.name;
        assert!(!c_name.is_null());

        use std::ffi::CStr;
        let c_str = unsafe { CStr::from_ptr(c_name) };
        let name = c_str.to_str().expect("Relation name not a valid UTF-8 string").to_string();

        let schema = self.schema.to_schema();

        Relation {
            name, schema
        }
    }

    pub fn from_relation(relation: Relation) -> ExtRelation {
        use logical_entities::schema::ExtSchema;
        let schema = ExtSchema::from_schema(relation.get_schema().clone());
        let name = relation.get_name().as_ptr() as *const c_char;

        ExtRelation {
            name,schema
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
                                                Column::new("a".to_string(),8),
                                                Column::new("b".to_string(), 8))
                                   ));

        assert_eq!(relation.get_name(),&"Test".to_string());

        assert_eq!(relation.get_columns().first().unwrap().get_name(),&"a".to_string());
        assert_eq!(relation.get_columns().last().unwrap().get_name(),&"b".to_string());

        assert_eq!(relation.get_schema(),&Schema::new(vec!(
            Column::new("a".to_string(),8),
            Column::new("b".to_string(), 8))));

        assert_eq!(relation.get_filename(),"test-data/Test.hsl".to_string());

        assert_eq!(relation.get_row_size(), 16);

        //assert_eq!(relation.get_total_size(), 0);
        //TODO: Support this in a unit test, not just integrated test
    }

    /*#[test]
    fn ext_relation_create() {
        use logical_entities::relation::ExtRelation;
        use logical_entities::relation::Relation;
        use logical_entities::schema::Schema;
        use logical_entities::column::Column;

        let relation = Relation::new("Test".to_string(),
                                     Schema::new(vec!(
                                         Column::new("a".to_string(),8),
                                         Column::new("b".to_string(), 8))
                                     ));

        let ext_relation = ExtRelation::from_relation(relation.clone());

        use std::ffi::CStr;
        unsafe {
            assert_eq!(CStr::from_ptr(ext_relation.name).to_str().unwrap(), "Test");
        }

        let r_relation = ext_relation.to_relation();

        assert_eq!(r_relation, relation);
    }*/
}
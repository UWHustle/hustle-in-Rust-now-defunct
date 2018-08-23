use logical_entities::schema::Schema;
use logical_entities::column::Column;

#[derive(Clone, Debug)]
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
#[derive(Copy, Clone)]
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
}


#[cfg(test)]
mod tests {
    #[test]
    fn relation_create() {
        use logical_entities::relation::Relation;
        use logical_entities::column::Column;
        //let columns =
        //let relation = Relation::new("test",);
        let column = Column::new("test".to_string(),8);
        assert_eq!(column.get_name(),&"test".to_string());
        assert_eq!(column.get_size(), 8);
    }

    #[test]
    fn c_column_create() {
        use logical_entities::column::ExtColumn;
        use logical_entities::column::Column;
        let column = Column::new("test".to_string(),8);

        use std::os::raw::c_char;
        use std::ffi::CStr;
        let c_column = ExtColumn::from_column(column);
        //unsafe {
         //   assert_eq!(CStr::from_ptr(c_column.name).to_str().unwrap(), "test");
        //}

        let r_column = c_column.to_column();
        assert_eq!(r_column.get_name(),&"test".to_string());
        assert_eq!(r_column.get_size(), 8);
    }
}
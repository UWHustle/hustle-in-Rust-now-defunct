use logical_entities::relation::Relation;
use physical_operators::Operator;
use type_system::*;

use super::storage::StorageManager;

extern crate csv;

pub struct ImportCsv {
    file_name: String,
    relation: Relation,
}

impl ImportCsv {
    pub fn new(file_name: String, relation: Relation) -> Self {
        ImportCsv {
            file_name,
            relation,
        }
    }
}

impl Operator for ImportCsv {
    fn get_target_relation(&self) -> Relation {
        self.relation.clone()
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Relation, String> {
        let mut reader = match csv::Reader::from_path(&self.file_name) {
            Ok(val) => val,
            Err(_err) => {
                return Err(String::from(format!(
                    "unable to open file '{}'",
                    self.file_name
                )))
            }
        };
        let record_count = reader.records().count() + 1;
        reader.seek(csv::Position::new()).unwrap();

        // Future optimization: create uninitialized Vec (this may require unsafe Rust)
        let size = self.relation.get_row_size() * record_count;
        let mut data: Vec<u8> = vec![0; size];

        let mut n: usize = 0;
        for result in reader.records() {
            let record = result.unwrap();

            for (i, column) in self.relation.get_columns().iter().enumerate() {
                let a = record.get(i).unwrap().to_string();

                let c = column.data_type().parse(&a)?;
                let size = c.size();
                data[n..n + size].clone_from_slice(c.un_marshall().data());
                n += size;
            }
        }

        storage_manager.put(self.relation.get_name(), &data);

        Ok(self.get_target_relation())
    }
}

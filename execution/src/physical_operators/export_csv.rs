use logical_entities::relation::Relation;
use physical_operators::Operator;
use type_system::borrowed_buffer::BorrowedBuffer;
use type_system::*;

use super::storage::StorageManager;

extern crate csv;

pub struct ExportCsv {
    file_name: String,
    relation: Relation,
}

impl ExportCsv {
    pub fn new(file_name: String, relation: Relation) -> Self {
        ExportCsv {
            file_name,
            relation,
        }
    }
}

impl Operator for ExportCsv {
    fn get_target_relation(&self) -> Relation {
        Relation::null()
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Relation, String> {
        let data = match storage_manager.get(self.relation.get_name()) {
            Some(data) => data,
            None => {
                return Err(format!(
                    "relation {} not found in storage manager",
                    self.relation.get_name()
                ))
            }
        };
        let mut writer = match csv::Writer::from_path(&self.file_name) {
            Ok(val) => val,
            Err(_err) => {
                return Err(String::from(format!(
                    "unable to open file '{}'",
                    self.file_name
                )))
            }
        };

        let mut i = 0;
        while i < data.len() {
            let mut r = Vec::new();

            for column in self.relation.get_columns() {
                let data_type = column.data_type();
                let value_length = data_type.size();
                let buffer: BorrowedBuffer =
                    BorrowedBuffer::new(&data[i..i + value_length], data_type.clone(), false);
                r.push(buffer.marshall().to_string());
                i += value_length;
            }
            writer.write_record(&r).unwrap();
        }
        writer.flush().unwrap();

        Ok(self.get_target_relation())
    }
}

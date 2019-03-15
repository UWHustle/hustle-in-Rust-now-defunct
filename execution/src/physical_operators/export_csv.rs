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

    pub fn get_file_name(&self) -> &String {
        &self.file_name
    }
}

impl Operator for ExportCsv {
    fn get_target_relation(&self) -> Relation {
        Relation::null()
    }

    fn execute(&self, storage_manager: &StorageManager) -> Relation {
        let data = storage_manager.get(self.relation.get_name()).unwrap();

        let mut writer = csv::Writer::from_path(self.get_file_name()).unwrap();

        let mut i = 0;
        while i < data.len() {
            let mut r = Vec::new();

            for column in self.relation.get_columns() {
                let type_id = column.get_datatype();
                let value_length = type_id.size();
                let buffer: BorrowedBuffer =
                    BorrowedBuffer::new(&data[i..i + value_length], type_id.clone(), false);
                r.push(buffer.marshall().to_string());
                i += value_length;
            }
            writer.write_record(&r).unwrap();
        }
        writer.flush().unwrap();

        self.get_target_relation()
    }
}

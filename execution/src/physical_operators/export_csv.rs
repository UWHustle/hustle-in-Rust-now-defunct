extern crate csv;
extern crate rand;

use logical_entities::relation::Relation;
use type_system::borrowed_buffer::BorrowedBuffer;
use type_system::*;

use physical_operators::Operator;

use storage_manager::StorageManager;

//#[derive(Debug)]
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

    fn execute(&self) -> Relation {
        let mut wtr = csv::Writer::from_path(self.get_file_name()).unwrap();

        let columns = self.relation.get_columns();
        let data = StorageManager::get_full_data(&self.relation.clone());

        let mut i = 0;
        while i < data.len() {
            let mut r = Vec::new();

            for column in columns {
                let type_id = column.get_datatype();
                let value_length = type_id.size();
                let buffer: BorrowedBuffer =
                    BorrowedBuffer::new(&data[i..i + value_length], type_id.clone(), false);
                r.push(buffer.marshall().to_string());
                i += value_length;
            }
            wtr.write_record(&r).unwrap();
        }
        wtr.flush().unwrap();

        self.get_target_relation()
    }
}

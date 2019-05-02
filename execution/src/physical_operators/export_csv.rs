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
        let schema = self.relation.get_schema();
        let schema_sizes = schema.to_size_vec();
        let record = storage_manager
            .get_with_schema(self.relation.get_name(), &schema_sizes)
            .unwrap();

        let mut writer = match csv::Writer::from_path(&self.file_name) {
            Ok(val) => val,
            Err(_err) => {
                return Err(String::from(format!(
                    "unable to open file '{}'",
                    self.file_name
                )));
            }
        };

        for block in record.blocks() {
            for row_i in 0..block.len() {
                let mut values: Vec<String> = vec![];
                for col_i in 0..schema.get_columns().len() {
                    let data = block.get_row_col(row_i, col_i).unwrap();
                    let data_type = schema.get_columns()[col_i].data_type();
                    let buff = BorrowedBuffer::new(&data, data_type, false);
                    values.push(buff.marshall().to_string())
                }
                writer.write_record(&values).unwrap();
            }
            writer.flush().unwrap()
        }

        Ok(self.get_target_relation())
    }
}

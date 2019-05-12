use logical_entities::relation::Relation;
use physical_operators::Operator;
use type_system::borrowed_buffer::BorrowedBuffer;
use type_system::*;

use super::storage::StorageManager;

extern crate csv;

pub struct ExportCsv {
    relation: Relation,
    filename: String,
}

impl ExportCsv {
    pub fn new(relation: Relation, filename: &str) -> Self {
        ExportCsv {
            relation,
            filename: String::from(filename),
        }
    }
}

impl Operator for ExportCsv {
    fn get_target_relation(&self) -> Relation {
        Relation::null()
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Relation, String> {
        let schema = self.relation.get_schema();
        let physical_relation = storage_manager
            .relational_engine()
            .get(self.relation.get_name())
            .unwrap();

        let mut writer = match csv::Writer::from_path(&self.filename) {
            Ok(val) => val,
            Err(_err) => {
                return Err(String::from(format!(
                    "unable to open file '{}'",
                    self.filename
                )));
            }
        };

        for block in physical_relation.blocks() {
            for row_i in 0..block.get_n_rows() {
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

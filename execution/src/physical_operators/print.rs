use logical_entities::relation::Relation;
use physical_operators::Operator;
use type_system::borrowed_buffer::BorrowedBuffer;
use type_system::*;

use super::storage::StorageManager;

pub struct Print {
    relation: Relation,
}

impl Print {
    pub fn new(relation: Relation) -> Print {
        Print { relation }
    }
}

impl Operator for Print {
    fn get_target_relation(&self) -> Option<Relation> {
        None
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Option<Relation>, String> {
        let schema = self.relation.get_schema();
        let width = 5;

        for column in schema.get_columns() {
            print!("|{value:>width$}", value = column.get_name(), width = width);
        }
        println!("|");

        let physical_relation = storage_manager
            .relational_engine()
            .get(self.relation.get_name())
            .unwrap();

        for block in physical_relation.blocks() {
            for row_i in 0..block.get_n_rows() {
                for col_i in 0..schema.get_columns().len() {
                    let data = block.get_row_col(row_i, col_i).unwrap();
                    let data_type = schema.get_columns()[col_i].data_type();
                    let buff = BorrowedBuffer::new(&data, data_type, false);
                    print!(
                        "|{value:>width$}",
                        value = buff.marshall().to_string(),
                        width = width
                    );
                }
                println!("|");
            }
        }

        Ok(self.get_target_relation())
    }
}

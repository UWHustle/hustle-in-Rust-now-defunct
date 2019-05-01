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
    fn get_target_relation(&self) -> Relation {
        Relation::null()
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Relation, String> {
        let schema = self.relation.get_schema();
        let schema_sizes = schema.to_size_vec();
        let record = storage_manager
            .get_with_schema(self.relation.get_name(), &schema_sizes)
            .unwrap();

        let width = 5;

        for column in schema.get_columns() {
            print!("|{value:>width$}", value = column.get_name(), width = width);
        }
        println!("|");

        for block in record.blocks() {
            for row_i in 0..block.len() {
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
            }
        }

        Ok(self.get_target_relation())
    }
}

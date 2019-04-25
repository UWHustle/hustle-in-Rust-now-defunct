use logical_entities::column::Column;
use logical_entities::relation::Relation;
use type_system::borrowed_buffer::*;
use type_system::*;

use super::storage::StorageManager;

// There's an off-by-one error somewhere in this operator when multi-threaded so setting this high.
// Reproduce by setting this low and running integrated tests.
pub const CHUNK_SIZE: usize = 1024 * 1024 * 1024 * 1024 * 1024;

pub struct SelectSum {
    relation: Relation,
    column: Column,
}

impl SelectSum {
    pub fn new(relation: Relation, column: Column) -> Self {
        SelectSum { relation, column }
    }

    pub fn execute(&self, storage_manager: &StorageManager) -> String {
        let data = storage_manager.get(self.relation.get_name()).unwrap();

        let mut sum = self.column.data_type().create_zero();
        let mut i = 0;
        while i < data.len() {
            for column in self.relation.get_columns() {
                let next_len = column.data_type().next_size(&data[i..]);
                if column.get_name() == self.column.get_name() {
                    let buffer =
                        BorrowedBuffer::new(&data[i..i + next_len], column.data_type(), false);
                    sum = sum.add(force_numeric(&*buffer.marshall()));
                }
                i += next_len;
            }
        }
        sum.to_string().to_string()
    }
}

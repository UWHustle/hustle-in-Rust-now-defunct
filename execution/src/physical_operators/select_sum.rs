use logical_entities::column::Column;
use logical_entities::relation::Relation;
use type_system::borrowed_buffer::*;
use type_system::*;

use storage_manager::StorageManager;

//There's an off-by-one error somewhere in this operator when multi-threaded so setting this high.
//Reproduce by setting this low and running integrated tests.
pub const CHUNK_SIZE: usize = 1024 * 1024 * 1024 * 1024 * 1024;

//#[derive(Debug)]
pub struct SelectSum {
    relation: Relation,
    column: Column,
}

impl SelectSum {
    pub fn new(relation: Relation, column: Column) -> Self {
        SelectSum { relation, column }
    }

    pub fn execute(&self) -> String {
        let total_size = self.relation.get_total_size();
        let data = StorageManager::get_full_data(&self.relation);
        let columns = self.relation.get_columns();

        let mut sum = self.column.get_datatype().create_zero();

        let mut i = 0;
        while i < total_size {
            for column in columns {
                let next_len = column.get_datatype().next_size(&data[i..]);
                if column.get_name() == self.column.get_name() {
                    let buffer =
                        BorrowedBuffer::new(&data[i..i + next_len], column.get_datatype(), false);
                    sum = sum.add(force_numeric(&*buffer.marshall()));
                }
                i += next_len;
            }
        }
        sum.to_string().to_string()
    }
}

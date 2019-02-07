use logical_entities::relation::Relation;
use logical_entities::column::Column;
use logical_entities::types::*;
use logical_entities::types::borrowed_buffer::*;

use storage_manager::StorageManager;

//There's an off-by-one error somewhere in this operator when multi-threaded so setting this high.
//Reproduce by setting this low and running integrated tests.
pub const CHUNK_SIZE: usize = 1024 * 1024 * 1024 * 1024 * 1024;


#[derive(Debug)]
pub struct SelectSum {
    relation: Relation,
    column: Column,
}

impl SelectSum {
    pub fn new(relation: Relation, column: Column) -> Self {
        SelectSum {
            relation,
            column,
        }
    }


    pub fn execute(&self) -> String {
        let total_size = self.relation.get_total_size();
        let data = StorageManager::get_full_data(&self.relation);
        let columns = self.relation.get_columns();

        let mut sum = self.column.get_datatype().create_zero();

        let mut i = 0;
        while i < total_size {
            for column in columns {
                if column.get_name() == self.column.get_name() {
                    // TODO: Won't work for non-constant size objects
                    let next_length = column.get_datatype().size();
                    let buffer = BorrowedBuffer::new(column.get_datatype(), false, &data[i..i + next_length]);
                    sum = sum.add(force_numeric(&*buffer.marshall()));
                }
            }
        }
        sum.to_str().to_string()
    }
}
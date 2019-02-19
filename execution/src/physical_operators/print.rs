use logical_entities::relation::Relation;
use type_system::borrowed_buffer::BorrowedBuffer;
use type_system::*;

use storage_manager::StorageManager;

use physical_operators::Operator;

pub const CHUNK_SIZE: usize = 1024 * 1024;

//#[derive(Debug)]
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

    fn execute(&self) -> Relation {
        let columns = self.relation.get_columns();
        let data = StorageManager::get_full_data(&self.relation.clone());

        let width = 5;
        for column in self.relation.get_schema().get_columns() {
            print!("|{value:>width$}", value = column.get_name(), width = width);
        }
        println!("|");

        let mut i = 0;
        while i < data.len() {
            for column in columns {
                let type_id = column.get_datatype();
                let value_length = type_id.next_size(&data[i..]);
                let buffer: BorrowedBuffer =
                    BorrowedBuffer::new(&data[i..i + value_length], type_id.clone(), false);
                let value_string = buffer.marshall().to_string();
                print!("|{value:>width$}", value = value_string, width = width);
                i += value_length;
            }
            println!("|");
        }

        self.get_target_relation()
    }
}

use logical_entities::relation::Relation;
use logical_entities::column::Column;

use storage_manager::StorageManager;

pub const CHUNK_SIZE:usize = 1024*1024;


#[derive(Debug)]
pub struct SelectSum {
    relation: Relation,
    column: Column,
}

impl SelectSum {
    pub fn new(relation: Relation, column:Column) -> Self {
        SelectSum {
            relation,column
        }
    }


    pub fn execute(&self) -> String{
        use std::thread;
        use std::sync::Arc;
        let mut children = vec![];

        let thread_relation = Arc::new(self.relation.clone());
        let column_name = Arc::new(self.column.get_name().clone());

        let mut my_chunk_start:isize = -1*(CHUNK_SIZE as isize);
        let mut my_chunk_end = 0;
        let mut my_chunk_length;


        let total_size = self.relation.get_total_size();

        while my_chunk_end < total_size {
            use std::cmp;
            let my_relation = thread_relation.clone();
            let my_target_column_name = column_name.clone();

            my_chunk_start += CHUNK_SIZE as isize;
            my_chunk_end = cmp::min(my_chunk_end + CHUNK_SIZE, total_size);
            my_chunk_length = my_chunk_end - my_chunk_start as usize;

            children.push(thread::spawn(move|| {

                let columns = my_relation.get_columns();
                let col_name = Arc::as_ref(&my_target_column_name);

                let data = StorageManager::get_chunk_data(&my_relation, my_chunk_start as usize, my_chunk_length);

                let mut i = 0;
                let mut s:Vec<u8> = Vec::new();
                use std::collections::HashMap;
                let mut column_sums = HashMap::new();

                while i < my_chunk_length {
                    for column in columns {
                        let next_length = column.get_datatype().get_next_length(&data[i..]);

                        if column.get_name() == col_name {
                            s = column.get_datatype().sum(&s, &data[i..i + next_length].to_vec()).0;
                            let value = column_sums.entry(col_name.clone()).or_insert(s.clone());
                            *value = s.clone();
                        }

                        i += next_length;
                    }
                }
                column_sums
            }));
        }

        let mut s:Vec<u8> = Vec::new();
        for child in children {
            let intermediate_sum = child.join().unwrap();
            s = match intermediate_sum.get(self.column.get_name()) {
                Some(sum) => self.column.get_datatype().sum(&s, sum).0,
                None => s,
            }
        }
        let final_result = self.column.get_datatype().to_string(&s);
        final_result
    }
}
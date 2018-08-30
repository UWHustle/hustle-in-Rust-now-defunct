use logical_entities::relation::Relation;

use std::time::{Instant};

extern crate memmap;
use std::mem;
use std::{
    fs::OpenOptions,
};

use logical_entities::types::integer::IntegerType;

pub const CHUNK_SIZE:usize = 1024*1024;

use logical_entities::column::Column;
use logical_entities::types::DataType;

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


    pub fn execute(&self) -> u128{
        //println!("{},{}", self.relation.get_name(), self.column.get_name());
        use std::thread;
        use std::sync::Arc;
        let now = Instant::now();
        let mut children = vec![];

        let thread_relation = Arc::new(self.relation.clone());
        let column_name = Arc::new(self.column.get_name().clone());

        let mut my_chunk_start:isize = -1*(CHUNK_SIZE as isize);
        let mut my_chunk_end = 0;
        let mut my_chunk_length;


        let total_size = self.relation.get_total_size();
        //println!("{}",total_size);
        //println!("{}",self.relation.get_filename());
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

                let f = OpenOptions::new()
                    .read(true)
                    .open(my_relation.get_filename())
                    .expect("Unable to open file");

                let data = unsafe {
                    memmap::MmapOptions::new()
                        .len(my_chunk_length)
                        .offset(my_chunk_start as usize)
                        .map(&f)
                        .expect("Could not access data from memory mapped file")
                };
                //println!("{}",my_relation.get_filename());

                let mut i = 0;
                let mut s:Vec<u8> = Vec::new();

                while i < my_chunk_length {
                    for column in columns {
                        let next_length = IntegerType::get_next_length(&data[i..]);

                        if column.get_name() == col_name {
                            s = IntegerType::sum(&s, &data[i..i+next_length].to_vec()).0;
                        }

                        i += next_length;
                    }
                }
                let t = IntegerType::to_int(&s);
                t
            }));
        }

        let mut intermediate_sums = vec![];
        for child in children {
            let intermediate_sum = child.join().unwrap();
            intermediate_sums.push(intermediate_sum);
        }
        let final_result = intermediate_sums.iter().sum::<u128>();
        println!("Final Sum: {}", final_result);
        println!("Finished Reading MemMap After {} seconds.", now.elapsed().as_secs());
        final_result
    }
}
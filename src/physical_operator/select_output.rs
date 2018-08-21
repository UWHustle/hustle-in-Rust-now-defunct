use logical_operator::logical_relation::LogicalRelation;
use logical_operator::row::Row;

use std::time::{Instant};

extern crate memmap;
use std::mem;
use std::{
    fs::OpenOptions,
};

pub const CHUNK_SIZE:usize = 1024*1024;

#[derive(Debug)]
pub struct SelectOutput {
    relation: LogicalRelation,
    result: Vec<Row>,
}

impl SelectOutput {
    pub fn new(relation: LogicalRelation) -> SelectOutput {
        let result = vec!();
        SelectOutput {
            relation,
            result,
        }
    }

    fn get_result(&mut self) -> &mut Vec<Row> {
        return &mut self.result;
    }

    pub fn print(&mut self) -> bool {
        let width = 5;
        for column in self.relation.get_schema().get_columns() {
            print!("|{value:>width$}",value=column.get_name(), width=width);
        }
        println!("|");
        for row in self.get_result() {
            for value in row.get_values() {
                print!("|{value:>width$}",value=value, width=width);
            }
            println!("|");
        }
        true
    }


    pub fn execute(&mut self) -> bool{
        let now = Instant::now();

        let total_size = self.relation.get_total_size();

        let my_chunk_start = 0;
        let my_chunk_length = total_size;

        let columns = self.relation.get_columns();

        let f = OpenOptions::new()
            .read(true)
            .open(self.relation.get_filename())
            .expect("Unable to open file");

        let data = unsafe {
            memmap::MmapOptions::new()
                .len(my_chunk_length)
                .offset(my_chunk_start as usize)
                .map(&f)
                .expect("Could not access data from memory mapped file")
        };

        let mut t_v : [u8; 8] = [0,0,0,0,0,0,0,0];
        let mut i = 0;
        while i < my_chunk_length {
            let mut row_values:Vec<u64> = vec!();

            for column in columns {
                t_v.clone_from_slice(&data[i..i+column.get_size()]);
                unsafe {
                    let _t_v_p = mem::transmute::<[u8; 8], u64>(t_v);
                    row_values.push(_t_v_p);
                }
                i += column.get_size();
            }

            let row = Row::new(self.relation.get_schema().clone(), row_values);
            self.result.push(row);
        }

        println!("Finished Select After {} milli-seconds.", now.elapsed().subsec_millis());
        true
    }
}
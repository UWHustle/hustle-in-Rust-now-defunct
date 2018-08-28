use logical_entities::relation::Relation;
use logical_entities::row::Row;

use std::time::{Instant};

extern crate memmap;
use std::mem;
use std::{
    fs::OpenOptions,
};

#[derive(Debug)]
pub struct Insert {
    relation: Relation,
    row: Row,
}

impl Insert {
    pub fn new(relation: Relation, row:Row) -> Self {
        Insert {
            relation,row
        }
    }


    pub fn execute(&self) -> bool{
        let now = Instant::now();

        let total_size = self.relation.get_total_size();
        let row_size = self.relation.get_row_size();

        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(self.relation.get_filename())
            .expect("Unable to open file");

        f.set_len((total_size + row_size) as u64).unwrap();

        let mut data = unsafe {
            memmap::MmapOptions::new()
                .offset(total_size)
                .len(row_size)
                .map_mut(&f)
                .expect("Could not access data from memory mapped file")
        };

        let mut n = 0;
        for (i, column) in self.row.get_schema().get_columns().iter().enumerate() {

            let a = self.row.get_values()[i];
            unsafe {
                let c = mem::transmute::<u64, [u8; 8]>(a);
                data[n..n + column.get_size()].clone_from_slice(&c); // 0  8
                n = n + column.get_size();
            }
        }

        true
    }
}
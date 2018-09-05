extern crate rand;
extern crate csv;

use logical_entities::relation::Relation;

extern crate memmap;
use std::{
    fs::OpenOptions,
    io::{Seek, SeekFrom, Write},
};

#[derive(Debug)]
pub struct RandomRelation {
    relation: Relation,
    row_count: usize
}

impl RandomRelation {
    pub fn new(relation: Relation, row_count: usize) -> Self {
        RandomRelation {
            relation,
            row_count
        }
    }

    pub fn execute(&self) -> bool {

        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.relation.get_filename())
            .expect("Unable to open file");

        // Allocate space in the file first
        f.seek(SeekFrom::Start((self.relation.get_row_size() * self.row_count) as u64)).unwrap();
        f.write_all(&[0]).unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();

        f.set_len((self.relation.get_row_size() * self.row_count) as u64).unwrap();

        let mut data = unsafe {
            memmap::MmapOptions::new()
                .map_mut(&f)
                .expect("Could not access data from memory mapped file")
        };

        let columns = self.relation.get_columns();
        let mut n : usize = 0;

        let mut insert_value = |data: &mut memmap::MmapMut| {
            for column in columns.iter() {
                let random_value = rand::random::<u32>().to_string();

                let (c,size) = column.get_datatype().parse_and_marshall(random_value);
                data[n..n + size].clone_from_slice(&c); // 0  8
                n = n + size;
            }
        };

        for _y in 0..self.row_count {
            insert_value(&mut data);
        }

        true
    }
}
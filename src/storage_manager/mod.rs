extern crate memmap;

use std::{
    fs::OpenOptions,
};

use logical_entities::relation::Relation;

pub struct StorageManager;

impl StorageManager {
    pub fn get_data(relation: Relation) -> memmap::Mmap {

        let total_size = relation.get_total_size();

        let my_chunk_start = 0;
        let my_chunk_length = total_size;

        let f = OpenOptions::new()
            .read(true)
            .open(relation.get_filename())
            .expect("Unable to open file");

        let data = unsafe {
            memmap::MmapOptions::new()
                .len(my_chunk_length)
                .offset(my_chunk_start as usize)
                .map(&f)
                .expect("Could not access data from memory mapped file")
        };

        return data;
    }
}
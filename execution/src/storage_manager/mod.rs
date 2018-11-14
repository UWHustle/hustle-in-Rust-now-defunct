extern crate memmap;
use std::{
    fs::OpenOptions,
    io::{Seek, SeekFrom, Write},
};

use logical_entities::relation::Relation;

pub struct StorageManager;

impl StorageManager {
    pub fn get_full_data(relation: &Relation) -> memmap::Mmap {

        let total_size = relation.get_total_size();
        let data = StorageManager::get_chunk_data(relation, 0, total_size);

        return data;
    }

    pub fn get_chunk_data(relation: &Relation, offset: usize, length: usize) -> memmap::Mmap {
        let f = OpenOptions::new()
            .read(true)
            .open(relation.get_filename())
            .expect("Unable to open file");

        let data = unsafe {
            memmap::MmapOptions::new()
                .len(length)
                .offset(offset as usize)
                .map(&f)
                .expect("Could not access data from memory mapped file")
        };

        return data;
    }

    pub fn create_relation(relation: &Relation, initial_len: usize) -> memmap::MmapMut {
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(relation.get_filename())
            .expect("Unable to open file");

        // Allocate space in the file first
        f.seek(SeekFrom::Start(initial_len as u64)).unwrap();
        f.write_all(&[0]).unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();

        f.set_len(initial_len as u64).unwrap();

        unsafe {
            memmap::MmapOptions::new()
                .map_mut(&f)
                .expect("Could not access data from memory mapped file")
        }
    }

    pub fn append_relation(relation: &Relation, appended_length: usize) -> memmap::MmapMut{
        let total_size = relation.get_total_size();

        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(relation.get_filename())
            .expect("Unable to open file");

        f.set_len((total_size + appended_length) as u64).unwrap();

        unsafe {
            memmap::MmapOptions::new()
                .offset(total_size)
                .len(appended_length)
                .map_mut(&f)
                .expect("Could not access data from memory mapped file")
        }
    }

    pub fn trim_relation(relation: &Relation, total_length:usize) -> () {
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(relation.get_filename())
            .expect("Unable to open file");
        f.set_len(total_length as u64);
    }
}
extern crate memmap;

use self::memmap::Mmap;
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::fs::{self, File};
use std::path::PathBuf;
use std::time::Instant;

struct LruBufferRecord {
    value: Mmap,
    pinned: bool,
}

impl LruBufferRecord {
    pub fn new(value: Mmap) -> LruBufferRecord {
        LruBufferRecord {
            value,
            pinned: false,
        }
    }
}

pub struct LruBuffer {
    k: u8,
    capacity: u64,
    fill: u64,
    records: HashMap<String, LruBufferRecord>,
    reference_log: HashMap<String, VecDeque<Instant>>,
    start_instant: Instant
}

impl LruBuffer {

    pub fn new(k: u8, capacity: u64) -> LruBuffer {
        LruBuffer {
            k,
            capacity,
            fill: 0,
            records: HashMap::new(),
            reference_log: HashMap::new(),
            start_instant: Instant::now()
        }
    }

    pub fn put(&mut self, key: &str, value: &[u8]) -> Result<(), Box<Error>> {
        // First persist the changes to storage
        let path = self.get_file_path(key);
        fs::write(path, value)?;

        // Easiest now to deload the old record and load the new record into the buffer
        // May eventually be able to perform updates without remapping the underlying file
        self.deload(key);
        self.load(key)?;
        Ok(())
    }

    pub fn get(&mut self, key: &str) -> Result<(usize, *const u8), Box<Error>> {
        let mmap = self.load(key)?;
        Ok((mmap.len(), mmap.as_ptr()))
    }

    pub fn delete(&mut self, key: &str) -> Result<(), Box<Error>> {
        let path = self.get_file_path(key);
        fs::remove_file(path)?;
        self.deload(key);
        Ok(())
    }

    pub fn pin(&mut self, key: &str) {
        self.records.get_mut(key).expect("Attempt to pin a nonexistent key").pinned = true;
    }

    pub fn release(&mut self, key: &str) {
        self.records.get_mut(key).expect("Attempt to release a nonexistent key").pinned = false;
    }

    fn load(&mut self, key: &str) -> Result<&Mmap, Box<Error>> {
        let time = Instant::now();

        // If the record is already in the buffer, update LRU log and return
        if self.records.contains_key(key) {
            self.log_reference(key, time);
            return Ok(&self.records.get(key).unwrap().value)
        }

        // The record must be loaded from storage
        let path = self.get_file_path(key);
        let file = File::open(path)?;
        let space_needed = file.metadata()?.len();

        // Remove the LRU-K record from the buffer until there is enough space
        while self.capacity - self.fill < space_needed {
            let key_to_remove = self.records
                .iter()
                .filter(|record| !record.1.pinned)
                .min_by_key(|record| self.reference_log.get(record.0.as_str()).unwrap().front().unwrap())
                .unwrap().0.clone();
            self.deload(key_to_remove.as_str());
        }

        // Load the record from storage
        let mmap = unsafe { Mmap::map(&file)? };
        self.records.insert(String::from(key), LruBufferRecord::new(mmap));
        self.fill += space_needed;

        // Initialize LRU log if it does not exist
        if !self.reference_log.contains_key(key) {
            let mut new_reference_log = VecDeque::new();
            for _i in 0..self.k {
                new_reference_log.push_back(self.start_instant);
            }
            self.reference_log.insert(String::from(key), new_reference_log);
        }

        // Update LRU log
        self.log_reference(key, time);

        // Return the buffered record
        Ok(&self.records.get(key).unwrap().value)
    }

    fn deload(&mut self, key: &str) {
        if let Some(removed_record) = self.records.remove(key) {
            self.fill -= removed_record.value.len() as u64;
        }
    }

    fn log_reference(&mut self, key: &str, time: Instant) {
        if let Some(log) = self.reference_log.get_mut(key) {
            log.pop_front();
            log.push_back(time);
        }
    }

    fn get_file_path(&mut self, key: &str) -> PathBuf {
        let mut path = PathBuf::from(key);
        path.set_extension("hustle");
        path
    }
}
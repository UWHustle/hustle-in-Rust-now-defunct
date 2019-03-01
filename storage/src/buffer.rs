extern crate memmap;
extern crate omap;

use self::memmap::Mmap;
use self::omap::OrderedHashMap;
use std::path::PathBuf;
use std::fs::{self, File};
use std::sync::{Arc, RwLock, Weak};
use std::mem;

struct BufferRecord {
    value: Arc<Mmap>
}

pub struct Buffer {
    capacity: usize,
    fill: usize,
    records: RwLock<OrderedHashMap<String, BufferRecord>>
}

impl BufferRecord {
    fn new(value: Arc<Mmap>) -> Self {
        BufferRecord {
            value
        }
    }
}

impl Buffer {
    pub fn new() -> Self {
        Self::with_capacity(1000)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Buffer {
            capacity,
            fill: 0,
            records: RwLock::new(OrderedHashMap::new())
        }
    }

    pub fn put(&self, key: &str, value: &[u8]) {
        let path = self.file_path(key);
        fs::write(&path, value).unwrap();
    }

    pub fn get(&self, key: &str) -> Option<Weak<Mmap>> {
        let records_read_guard = self.records.read().unwrap();
        records_read_guard.get(key)
            .map(|record| record.value.clone())
            .or_else(|| {
                let path = self.file_path(key);
                let file = File::open(path).ok()?;
                let mmap = unsafe { Mmap::map(&file).ok()? };
                Some(Arc::new(mmap))
            })
            .map(|value| {
                mem::drop(records_read_guard);
                self.records.write().unwrap().insert_front(key.to_string(), BufferRecord::new(value.clone()));
                Arc::downgrade(&value)
            })
    }

    pub fn delete(&self, key: &str) {
        let mut records_guard = self.records.write().unwrap();
        records_guard.remove(key);
        let path = self.file_path(key);
        fs::remove_file(path).unwrap();
    }

    fn file_path(&self, key: &str) -> PathBuf {
        let mut path = PathBuf::from(key);
        path.set_extension("hustle");
        path
    }
}

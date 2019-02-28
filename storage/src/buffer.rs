extern crate memmap;
extern crate omap;

use self::memmap::Mmap;
use self::omap::OrderedHashMap;
use std::path::PathBuf;
use std::fs::{self, File};
use std::sync::RwLock;

struct BufferRecord {
    value: Mmap
}

pub struct Buffer {
    capacity: usize,
    fill: usize,
    records: RwLock<OrderedHashMap<String, BufferRecord>>
}

impl BufferRecord {
    fn new(value: Mmap) -> Self {
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

    pub fn put(&self, key: &str, value: &[u8]) -> Option<()> {
        let path = self.file_path(key);
        fs::write(&path, value);
        None
    }

    pub fn get(&self, key: &str) -> Option<Mmap> {
        let path = self.file_path(key);
        let file = File::open(path).ok()?;
        let mmap = unsafe { Mmap::map(&file).ok()? };
        Some(mmap)
    }

    pub fn delete(&self, key: &str) {
        let path = self.file_path(key);
        fs::remove_file(path);
    }

    fn file_path(&self, key: &str) -> PathBuf {
        let mut path = PathBuf::from(key);
        path.set_extension("hustle");
        path
    }
}

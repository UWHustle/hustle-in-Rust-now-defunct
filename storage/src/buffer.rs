extern crate memmap;
extern crate omap;

use self::memmap::Mmap;
use self::omap::OrderedHashMap;
use std::path::PathBuf;
use std::fs::{self, File};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockWriteGuard, Weak};
use std::mem;
use std::cmp::{max, min};

struct BufferRecord {
    value: Arc<Mmap>,
    reference: RwLock<bool>
}

pub struct Buffer {
    capacity: usize,
    t1: RwLock<OrderedHashMap<String, BufferRecord>>,
    t2: RwLock<OrderedHashMap<String, BufferRecord>>,
    b1: Mutex<OrderedHashMap<String, ()>>,
    b2: Mutex<OrderedHashMap<String, ()>>,
    p: Mutex<usize>
}

impl BufferRecord {
    fn new(value: Arc<Mmap>) -> Self {
        BufferRecord {
            value,
            reference: RwLock::new(false)
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
            t1: RwLock::new(OrderedHashMap::new()),
            t2: RwLock::new(OrderedHashMap::new()),
            b1: Mutex::new(OrderedHashMap::new()),
            b2: Mutex::new(OrderedHashMap::new()),
            p: Mutex::new(0)
        }
    }

    pub fn put(&self, key: &str, value: &[u8]) {
        let path = self.file_path(key);
        fs::write(&path, value).unwrap();
    }

    pub fn get(&self, key: &str) -> Option<Arc<Mmap>> {
        // Request read locks on cache lists.
        let t1_read_guard = self.t1.read().unwrap();
        let t2_read_guard = self.t2.read().unwrap();

        if let Some(record) = t1_read_guard.get(key).or(t2_read_guard.get(key)) {
            // Cache hit. Set reference bit to 1 and return.
            *record.reference.write().unwrap() = true;
            return Some(record.value.clone());
        }

        // Cache miss. Drop read locks on cache lists.
        mem::drop(t1_read_guard);
        mem::drop(t2_read_guard);

        // Read in the file from storage
        let path = self.file_path(key);
        let file = File::open(path).ok()?;
        let mmap = Arc::new(unsafe { Mmap::map(&file).ok()? });
        let record = BufferRecord::new(mmap);
        let value = record.value.clone();

        // Request write locks on all lists.
        let mut t1_write_guard = self.t1.write().unwrap();
        let mut t2_write_guard = self.t2.write().unwrap();
        let mut b1_guard = self.b1.lock().unwrap();
        let mut b2_guard = self.b2.lock().unwrap();
        let mut p_guard = self.p.lock().unwrap();

        // TODO: Check if another thread loaded the page into the cache while waiting.

        let cache_directory_miss = !b1_guard.contains_key(key)
            && !b2_guard.contains_key(key);

        if t1_write_guard.len() + t2_write_guard.len() == self.capacity {
            // Cache is full. Replace a page from the cache.
            self.replace(&mut t1_write_guard, &mut t2_write_guard,
                         &mut b1_guard, &mut b2_guard);

            // Cache directory replacement.
            if cache_directory_miss {
                if t1_write_guard.len() + b1_guard.len() == self.capacity {
                    // Discard the LRU page in B1.
                    b1_guard.pop_front();

                } else if t1_write_guard.len() + t2_write_guard.len()
                    + b1_guard.len() + b2_guard.len() == 2 * self.capacity {
                    // Discard the LRU page in B2.
                    b2_guard.pop_front();
                }
            }
        }

        if cache_directory_miss {
            // Move the page to the back of T1.
            t1_write_guard.push_back(key.to_string(), record);

        } else if b1_guard.contains_key(key) {
            // B1 cache directory hit. Increase the target size for T1.
            *p_guard = min(*p_guard + max(1, b2_guard.len() / b1_guard.len()), self.capacity);

            // Move the page to the back of T2.
            b1_guard.remove(key);
            t2_write_guard.push_back(key.to_string(), record);

        } else {
            // B2 cache directory hit. Decrease the target size for T2.
            *p_guard = max(*p_guard - max(1, b1_guard.len() / b2_guard.len()), 0);

            // Move the page to the back of T2.
            b2_guard.remove(key);
            t2_write_guard.push_back(key.to_string(), record);
        }

        // Return the buffered byte array.
        Some(value)
    }

    pub fn delete(&self, key: &str) {

        // Request write locks on all lists.
        let mut t1_write_guard = self.t1.write().unwrap();
        let mut t2_write_guard = self.t2.write().unwrap();
        let mut b1_guard = self.b1.lock().unwrap();
        let mut b2_guard = self.b2.lock().unwrap();
//        let mut records_guard = self.records.write().unwrap();
//        records_guard.remove(key);
//        let path = self.file_path(key);
//        fs::remove_file(path).unwrap();
    }

    fn file_path(&self, key: &str) -> PathBuf {
        let mut path = PathBuf::from(key);
        path.set_extension("hustle");
        path
    }

    fn replace(&self,
               t1: &mut RwLockWriteGuard<OrderedHashMap<String, BufferRecord>>,
               t2: &mut RwLockWriteGuard<OrderedHashMap<String, BufferRecord>>,
               b1: &mut MutexGuard<OrderedHashMap<String, ()>>,
               b2: &mut MutexGuard<OrderedHashMap<String, ()>>) {
        if t1.len() >= max(1, *self.p.lock().unwrap()) {
            // T1 is at or above target size. Pop the front of T1.
            let (t1_front_key, t1_front_record) = t1.pop_front().unwrap();

            if !*t1_front_record.reference.read().unwrap()
                && Arc::strong_count(&t1_front_record.value) == 1 {
                // Page reference bit is 0 and no other threads are using it.
                // Replace this page and push the key to the MRU position of B1.
                b1.push_back(t1_front_key, ());
            } else {
                // Set the page reference bit to 0. Push the record to the back of T2.
                *t1_front_record.reference.write().unwrap() = false;
                t2.push_back(t1_front_key, t1_front_record);
            }

        } else {
            // Pop the front of T2.
            let (t2_front_key, t2_front_record) = t2.pop_front().unwrap();

            if !*t2_front_record.reference.read().unwrap()
                && Arc::strong_count(&t2_front_record.value) == 1 {
                // Page reference bit is 0 and no other threads are using it.
                // Replace this page and push the key to the MRU position of B2.
                b2.push_back(t2_front_key, ());
            } else {
                // Set the page reference bit to 0. Push the record to the back of T2.
                *t2_front_record.reference.write().unwrap() = false;
                t2.push_back(t2_front_key, t2_front_record);
            }
        }
    }
}

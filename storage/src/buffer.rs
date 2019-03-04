extern crate memmap;
extern crate omap;

use self::memmap::Mmap;
use self::omap::OrderedHashMap;
use std::path::PathBuf;
use std::fs::{self, File};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, RwLockWriteGuard};
use std::mem;
use std::cmp::{max, min};
use std::collections::HashSet;
use std::ops::Deref;

const DEFAULT_CAPACITY: usize = 1000;
const TEMP_RECORD_PREFIX: char = '$';

pub struct Value {
    data: Arc<Mmap>,
    rc: Arc<(Mutex<u64>, Condvar)>
}

struct BufferRecord {
    value: Value,
    reference: RwLock<bool>,
}

pub struct Buffer {
    capacity: usize,
    anon_ctr: Mutex<u64>,
    locked: (Mutex<HashSet<String>>, Condvar),
    t1: RwLock<OrderedHashMap<String, BufferRecord>>,
    t2: RwLock<OrderedHashMap<String, BufferRecord>>,
    b1: Mutex<OrderedHashMap<String, ()>>,
    b2: Mutex<OrderedHashMap<String, ()>>,
    p: Mutex<usize>
}

impl Clone for Value {
    fn clone(&self) -> Self {
        let mut rc_guard = self.rc.0.lock().unwrap();
        *rc_guard += 1;
        Value {
            data: self.data.clone(),
            rc: self.rc.clone()
        }
    }
}

impl Deref for Value {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.data
    }
}

impl Drop for Value {
    fn drop(&mut self) {
        // Decrement the reference count when Value is dropped.
        let &(ref rc_lock, ref cvar) = &*self.rc;
        let mut rc_guard = rc_lock.lock().unwrap();
        *rc_guard = rc_guard.saturating_sub(1);
        cvar.notify_all();
    }
}

impl Value {
    fn new(data: Arc<Mmap>) -> Self {
        Value {
            data,
            rc: Arc::new((Mutex::new(1), Condvar::new()))
        }
    }
}

impl BufferRecord {
    fn new(data: Arc<Mmap>) -> Self {
        BufferRecord {
            value: Value::new(data),
            reference: RwLock::new(false)
        }
    }
}

impl Buffer {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Buffer {
            capacity,
            anon_ctr: Mutex::new(0),
            locked: (Mutex::new(HashSet::new()), Condvar::new()),
            t1: RwLock::new(OrderedHashMap::new()),
            t2: RwLock::new(OrderedHashMap::new()),
            b1: Mutex::new(OrderedHashMap::new()),
            b2: Mutex::new(OrderedHashMap::new()),
            p: Mutex::new(0)
        }
    }

    pub fn put(&self, key: &str, value: &[u8]) {
        // If the file is locked, wait until it becomes unlocked.
        let mut locked = self.await_unlock(key);
        locked.insert(key.to_string());
        mem::drop(locked);

        // Remove the old file from storage.
        unsafe { self.delete_unlocked(key); }

        // Write the new file to storage and load it into the buffer.
        let path = self.file_path(key);
        fs::write(path, value).expect(format!("Error writing {} to storage.", key).as_str());

        unsafe { self.load_unlocked(key); }

        // Unlock the file and notify other threads.
        self.unlock(key);
    }

    pub fn put_anon(&self, value: &[u8]) -> String {
        // Generate a unique key, prefixed with the reserved character.
        let mut anon_ctr = self.anon_ctr.lock().unwrap();
        let key = format!("{}{}", TEMP_RECORD_PREFIX, *anon_ctr);
        *anon_ctr = anon_ctr.wrapping_add(1);
        mem::drop(anon_ctr);

        // Put the value and return the key.
        self.put(key.as_str(), value);
        key
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        // Request read locks on cache lists.
        let t1_read_guard = self.t1.read().unwrap();
        let t2_read_guard = self.t2.read().unwrap();

        if let Some(record) = t1_read_guard.get(key).or(t2_read_guard.get(key)) {
            // Cache hit. Set reference bit to 1.
            *record.reference.write().unwrap() = true;

            return Some(record.value.clone());
        }

        // Cache miss. Drop read locks on cache lists.
        mem::drop(t1_read_guard);
        mem::drop(t2_read_guard);

        // If the file is locked, wait until it becomes unlocked.
        let mut locked = self.await_unlock(key);

        // It is now safe to access the file. Lock the file until the load completes.
        locked.insert(key.to_string());
        mem::drop(locked);

        // Load the file from storage.
        let value = unsafe { self.load_unlocked(key) };

        // Unlock the file and notify other threads.
        self.unlock(key);

        // Return the buffered byte array.
        value
    }

    pub fn delete(&self, key: &str) {
        // If the file is locked, wait until it becomes unlocked.
        let mut locked = self.await_unlock(key);
        locked.insert(key.to_string());
        mem::drop(locked);

        unsafe { self.delete_unlocked(key); }

        // Unlock the file and notify other threads.
        self.unlock(key);
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
               b2: &mut MutexGuard<OrderedHashMap<String, ()>>,
               p: &MutexGuard<usize>) {
        if t1.len() >= max(1, **p) {
            // T1 is at or above target size. Pop the front of T1.
            let (t1_front_key, t1_front_record) = t1.pop_front().unwrap();

            if !*t1_front_record.reference.read().unwrap()
                && *t1_front_record.value.rc.0.lock().unwrap() == 1 {
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
                && *t2_front_record.value.rc.0.lock().unwrap() == 1 {
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

    /// Loads the file from storage into the buffer. This function is marked unsafe because it does
    /// not guarantee that another thread will not be concurrently modifying the file. Only use
    /// this when the file is locked.
    unsafe fn load_unlocked(&self, key: &str) -> Option<Value> {
        // Load the file from storage
        let path = self.file_path(key);
        let file = File::open(path).ok()?;
        let mmap = Arc::new(Mmap::map(&file).ok()?);
        let record = BufferRecord::new(mmap);

        let value = record.value.clone();

        // Request write locks on all lists.
        let mut t1_write_guard = self.t1.write().unwrap();
        let mut t2_write_guard = self.t2.write().unwrap();
        let mut b1_guard = self.b1.lock().unwrap();
        let mut b2_guard = self.b2.lock().unwrap();
        let mut p_guard = self.p.lock().unwrap();

        let cache_directory_miss = !b1_guard.contains_key(key)
            && !b2_guard.contains_key(key);

        if t1_write_guard.len() + t2_write_guard.len() == self.capacity {
            // Cache is full. Replace a page from the cache.
            self.replace(&mut t1_write_guard, &mut t2_write_guard,
                         &mut b1_guard, &mut b2_guard, &p_guard);

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

        Some(value)
    }

    /// Remove the page associated with the key from the buffer and delete the underlying file on
    /// storage. This function is marked unsafe because it does not guarantee that another thread
    /// will not be concurrently reading the file. Only use this when the file is locked.
    unsafe fn delete_unlocked(&self, key: &str) {
        if let Some(record) = self.t1.write().unwrap().remove(key)
            .or(self.t2.write().unwrap().remove(key)) {

            // Wait for the reference count to drop to 0.
            let &(ref rc_lock, ref cvar) = &*record.value.rc;
            let mut rc_guard = rc_lock.lock().unwrap();
            while *rc_guard > 1 {
                rc_guard = cvar.wait(rc_guard).unwrap();
            }

        } else {
            self.b1.lock().unwrap().remove(key).or(self.b2.lock().unwrap().remove(key));
        }

        // Delete the file.
        let path = self.file_path(key);
        if path.exists() {
            fs::remove_file(path).expect(format!("Error deleting {}.", key).as_str());
        }
    }

    fn unlock(&self, key: &str) {
        let &(ref lock, ref cvar) = &self.locked;
        let mut locked = lock.lock().unwrap();
        locked.remove(key);
        cvar.notify_all();
    }

    fn await_unlock(&self, key: &str) -> MutexGuard<HashSet<String>> {
        let &(ref lock, ref cvar) = &self.locked;
        let mut locked = lock.lock().unwrap();
        while locked.contains(key) {
            locked = cvar.wait(locked).unwrap();
        }
        locked
    }
}

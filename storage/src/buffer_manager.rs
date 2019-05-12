extern crate byteorder;
extern crate memmap;
extern crate omap;

use self::byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use self::memmap::MmapMut;
use self::omap::OrderedHashMap;
use std::ops::Deref;
use std::fs;
use std::path::PathBuf;
use std::mem;
use std::fs::OpenOptions;
use std::cmp::{max, min};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, RwLockWriteGuard};
use std::io::{Write};
use relational_block::RelationalBlock;
use memmap::Mmap;

/// A wrapper around `Block` to keep track of information used by the cache.
struct CacheBlock {
    block: RelationalBlock,
    reference: RwLock<bool>
}

impl CacheBlock {
    fn new(block: RelationalBlock) -> Self {
        CacheBlock {
            block,
            reference: RwLock::new(false)
        }
    }
}

/// A buffer that reads and writes to storage and maintains a cache in memory. The `Buffer` is
/// implemented as a key-value store, where the keys are strings and the values (pages) are
/// uniformly sized arrays of bytes. The page replacement policy for the cache is Clock with
/// Adaptive Replacement ([CAR](https://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.105.6057)).
/// The `Buffer` itself is thread-safe. However, it does not guard against concurrent reads and
/// writes to the same key-value. This must be handled by an external concurrency control module.
pub struct BufferManager {
    capacity: usize,
    t1: RwLock<OrderedHashMap<String, CacheBlock>>,
    t2: RwLock<OrderedHashMap<String, CacheBlock>>,
    b1: Mutex<OrderedHashMap<String, ()>>,
    b2: Mutex<OrderedHashMap<String, ()>>,
    p: Mutex<usize>
}

impl BufferManager {
    /// Creates a new `Buffer` with the specified capacity. The `Buffer` will hold at maximum
    /// `capacity` pages in memory.
    pub fn with_capacity(capacity: usize) -> Self {
        BufferManager {
            capacity,
            t1: RwLock::new(OrderedHashMap::new()),
            t2: RwLock::new(OrderedHashMap::new()),
            b1: Mutex::new(OrderedHashMap::new()),
            b2: Mutex::new(OrderedHashMap::new()),
            p: Mutex::new(0)
        }
    }

    /// Writes the key-value pair directly to storage without loading it into the cache. If a value
    /// for `key` already exists, it is overwritten with the new `value`.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage::Buffer;
    /// let buffer = Buffer::with_capacity(10);
    /// buffer.write("key_write", b"value");
    /// assert_eq!(&buffer.get("key_write").unwrap()[0..5], b"value");
    /// buffer.erase("key_write");
    /// ```
    pub fn write_uncached(&self, key: &str, value: &[u8]) {
        let path = Self::file_path(key);
        fs::write(&path, value);
    }

    pub fn get_uncached(&self, key: &str) -> Option<Mmap> {
        let path = Self::file_path(key);
        let file = OpenOptions::new()
            .read(true)
            .open(&path)
            .ok()?;
        unsafe { Mmap::map(&file).ok() }
    }

    /// Loads the value for `key` into the cache and returns a reference if it exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage::Buffer;
    /// let buffer = Buffer::with_capacity(10);
    /// buffer.write("key_get", b"value");
    /// assert_eq!(&buffer.get("key_get").unwrap()[0..5], b"value");
    /// assert!(buffer.get("nonexistent_key").is_none());
    /// buffer.erase("key_get");
    /// ```
    pub fn get(&self, key: &str) -> Option<RelationalBlock> {
        // Request read locks on cache lists.
        let t1_read_guard = self.t1.read().unwrap();
        let t2_read_guard = self.t2.read().unwrap();

        if let Some(buffer_block) = t1_read_guard.get(key).or(t2_read_guard.get(key)) {
            // Cache hit. Set reference bit to 1.
            *buffer_block.reference.write().unwrap() = true;
            return Some(buffer_block.block.clone());
        }

        // Cache miss. Drop read locks on cache lists.
        mem::drop(t1_read_guard);
        mem::drop(t2_read_guard);

        // Load the file from storage.
        let value = self.load(key);

        // Return the cached byte array.
        value
    }

    /// Removes the block associated with the key from the cache and deletes the underlying file on
    /// storage.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage::Buffer;
    /// let buffer = Buffer::with_capacity(10);
    /// buffer.write("key_erase", b"value");
    /// assert!(buffer.exists("key_erase"));
    /// buffer.erase("key_erase");
    /// assert!(!buffer.exists("key_erase"));
    /// ```
    pub fn erase(&self, key: &str) {
        // Remove the block from the cache and cache directory
        if self.t1.write().unwrap().remove(key)
            .or(self.t2.write().unwrap().remove(key)).is_none() {

            self.b1.lock().unwrap().remove(key)
                .or(self.b2.lock().unwrap().remove(key));
        }

        // Delete the file.
        let path = Self::file_path(key);
        fs::remove_file(&path);
    }

    /// Returns true if the key-value pair exists, regardless of whether it is cached.
    pub fn exists(&self, key: &str) -> bool {
        let path = Self::file_path(key);
        path.exists()
    }

    /// Returns true if the key-value pair exists and is cached.
    pub fn is_cached(&self, key: &str) -> bool {
        self.t1.read().unwrap().contains_key(key) || self.t2.read().unwrap().contains_key(key)
    }

    /// Loads the file from storage into the cache.
    fn load(&self, key: &str) -> Option<RelationalBlock> {
        // Load the file from storage
        let path = Self::file_path(key);
        let block = RelationalBlock::try_from_file(&path)?;
        let buffer_block = CacheBlock::new(block.clone());

        // Request write locks on all lists.
        let mut t1_write_guard = self.t1.write().unwrap();
        let mut t2_write_guard = self.t2.write().unwrap();
        let mut b1_guard = self.b1.lock().unwrap();
        let mut b2_guard = self.b2.lock().unwrap();
        let mut p_guard = self.p.lock().unwrap();

        // Another thread may have loaded the block into the cache while this one was waiting for
        // the write locks. If this is the case, return the cached block.
        if let Some(buffer_block) = t1_write_guard.get(key).or(t2_write_guard.get(key)) {
            // Cache hit. Set reference bit to 1.
            *buffer_block.reference.write().unwrap() = true;
            return Some(buffer_block.block.clone());
        };

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
                    b1_guard.pop_front().unwrap().0;

                } else if t1_write_guard.len() + t2_write_guard.len()
                    + b1_guard.len() + b2_guard.len() == 2 * self.capacity {
                    // Discard the LRU page in B2.
                    b2_guard.pop_front();
                };
            };
        };

        if cache_directory_miss {
            // Move the page to the back of T1.
            t1_write_guard.push_back(key.to_string(), buffer_block);

        } else if b1_guard.contains_key(key) {
            // B1 cache directory hit. Increase the target size for T1.
            *p_guard = min(*p_guard + max(1, b2_guard.len() / b1_guard.len()), self.capacity);

            // Move the page to the back of T2.
            b1_guard.remove(key);
            t2_write_guard.push_back(key.to_string(), buffer_block);

        } else {
            // B2 cache directory hit. Decrease the target size for T2.
            *p_guard = max(*p_guard - max(1, b1_guard.len() / b2_guard.len()), 0);

            // Move the page to the back of T2.
            b2_guard.remove(key);
            t2_write_guard.push_back(key.to_string(), buffer_block);
        };

        Some(block)
    }

    /// Removes the approximated least recently used page from the cache.
    fn replace(&self,
               t1: &mut RwLockWriteGuard<OrderedHashMap<String, CacheBlock>>,
               t2: &mut RwLockWriteGuard<OrderedHashMap<String, CacheBlock>>,
               b1: &mut MutexGuard<OrderedHashMap<String, ()>>,
               b2: &mut MutexGuard<OrderedHashMap<String, ()>>,
               p: &MutexGuard<usize>) {
        if t1.len() >= max(1, **p) {
            // T1 is at or above target size. Pop the front of T1.
            let (t1_front_key, t1_front_block) = t1.pop_front().unwrap();

            if !*t1_front_block.reference.read().unwrap()
                && *t1_front_block.block.get_reference_count().0.lock().unwrap() == 1 {
                // Page reference bit is 0 and no other threads are using it.
                // Replace this page and push the key to the MRU position of B1.
                b1.push_back(t1_front_key, ());
            } else {
                // Set the page reference bit to 0. Push the block to the back of T2.
                *t1_front_block.reference.write().unwrap() = false;
                t2.push_back(t1_front_key, t1_front_block);
            }

        } else {
            // Pop the front of T2.
            let (t2_front_key, t2_front_block) = t2.pop_front().unwrap();

            if !*t2_front_block.reference.read().unwrap()
                && *t2_front_block.block.get_reference_count().0.lock().unwrap() == 1 {
                // Page reference bit is 0 and no other threads are using it.
                // Replace this page and push the key to the MRU position of B2.
                b2.push_back(t2_front_key, ());
            } else {
                // Set the page reference bit to 0. Push the block to the back of T2.
                *t2_front_block.reference.write().unwrap() = false;
                t2.push_back(t2_front_key, t2_front_block);
            }
        }
    }

    fn file_path(key: &str) -> PathBuf {
        let name =  format!("{}.kv.hsl", key);
        let mut path = PathBuf::from(name);
        path
    }
}
extern crate byteorder;
extern crate memmap;
extern crate omap;

use self::byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use self::memmap::{Mmap, MmapMut};
use self::omap::OrderedHashMap;
use std::ops::Deref;
use std::fs;
use std::path::PathBuf;
use std::mem;
use std::fs::{File, OpenOptions};
use std::cmp::{max, min};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, RwLockWriteGuard};
use core::borrow::BorrowMut;
use std::io::{Write, Cursor, Seek, SeekFrom};

pub const BLOCK_SIZE: usize = 1000;
const HEADER_SIZE: usize = 4;

/// The unit of storage and replacement in the cache. It is assumed that all `Block`s in the buffer
/// are the same size, but this is not enforced.
pub struct Block {
    value: *mut MmapMut,
    rc: Arc<(Mutex<u64>, Condvar)>
}

impl Deref for Block {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe { &(*self.value)[HEADER_SIZE..] }
    }
}

impl Block {
    pub fn update(&self, offset: usize, value: &[u8]) {
        if offset + value.len() > self.len() {
            panic!("Offset {} and value len {} out of range.", offset, value.len());
        }
        let offset_in_block = offset + HEADER_SIZE;
        unsafe {
            (*self.value)[offset_in_block..offset_in_block + value.len()].copy_from_slice(value);
            (*self.value).flush();
        }
    }

    pub fn len(&self) -> usize {
        unsafe { (*self.value).as_ref().read_u32::<BigEndian>().unwrap() as usize }
    }

    fn write(&self, value: &[u8]) {
        let mut len = vec![];
        len.write_u32::<BigEndian>(value.len() as u32).unwrap();
        unsafe {
            (*self.value)[..HEADER_SIZE].copy_from_slice(&len);
            (*self.value)[HEADER_SIZE..HEADER_SIZE + value.len()].copy_from_slice(value);
            (*self.value).flush();
        }
    }

    fn load(key: &str) -> Option<Block> {
        let path = Self::file_path(key);
        let file = OpenOptions::new().read(true).write(true).open(&path).ok()?;
        file.set_len((HEADER_SIZE + BLOCK_SIZE) as u64);
        let mmap = unsafe { MmapMut::map_mut(&file).ok()? };
        Some(Block {
            value: Box::into_raw(Box::new(mmap)),
            rc: Arc::new((Mutex::new(1), Condvar::new()))
        })
    }

    fn write_direct(key: &str, value: &[u8]) {
        // Serialize the length of the value. This is the header of the block.
        let mut len = vec![];
        len.write_u32::<BigEndian>(value.len() as u32).unwrap();

        // Write the data to storage.
        let path = Self::file_path(key);
        let mut file = OpenOptions::new().create(true).write(true).open(&path)
            .expect("Error opening file.");
        file.write(&len);
        file.write(value);
    }

    fn erase(key: &str) {
        let path = Self::file_path(key);
        if path.exists() {
            fs::remove_file(path).expect(format!("Error deleting {}.", key).as_str());
        }
    }

    fn exists(key: &str) -> bool {
        Self::file_path(key).exists()
    }

    /// Returns the formatted file path.
    fn file_path(key: &str) -> PathBuf {
        let mut path = PathBuf::from(key);
        path.set_extension("hsl");
        path
    }
}

impl Drop for Block {
    fn drop(&mut self) {
        // Decrement the reference count when Value is dropped.
        let &(ref rc_lock, ref cvar) = &*self.rc;
        let mut rc_guard = rc_lock.lock().unwrap();
        *rc_guard -= 1;
        cvar.notify_all();

        if *rc_guard == 0 {
            // This was the last reference to the Block.
            // Convert the raw pointer back into a Box to release the memory.
            unsafe { Box::from_raw(self.value) };
        }
    }
}

impl Clone for Block {
    fn clone(&self) -> Self {
        let mut rc_guard = self.rc.0.lock().unwrap();
        *rc_guard += 1;
        Block {
            value: self.value,
            rc: self.rc.clone()
        }
    }
}

unsafe impl Send for Block {}

unsafe impl Sync for Block {}

struct BufferBlock {
    block: Block,
    reference: RwLock<bool>
}

impl BufferBlock {
    fn new(block: Block) -> Self {
        BufferBlock {
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
pub struct Buffer {
    capacity: usize,
    t1: RwLock<OrderedHashMap<String, BufferBlock>>,
    t2: RwLock<OrderedHashMap<String, BufferBlock>>,
    b1: Mutex<OrderedHashMap<String, ()>>,
    b2: Mutex<OrderedHashMap<String, ()>>,
    p: Mutex<usize>
}

impl Buffer {
    /// Creates a new `Buffer` with the specified capacity. The `Buffer` will hold at maximum
    /// `capacity` pages in memory.
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

    /// Writes the key-value pair to storage and loads it into the cache. If a value for `key`
    /// already exists, it is overwritten with the new `value`. The value is assumed to be the same
    /// size as other values already written, but this is not enforced.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage::Buffer;
    /// let buffer = Buffer::with_capacity(10);
    /// buffer.write("key", b"value");
    /// ```
    pub fn write(&self, key: &str, value: &[u8]) {
        if let Some(block) = self.get(key) {
            if value.len() > BLOCK_SIZE {
                panic!("Value to be written is larger than the block size.")
            }

            // Update the existing value.
            block.write(value);
        } else {
            // Write the new file directly to storage and load it into the cache.
            Block::write_direct(key, value);
            self.load(key);
        }
    }

    /// Loads the value for `key` into the cache and returns a reference if it exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage::Buffer;
    /// let buffer = Buffer::with_capacity(10);
    /// buffer.write("key", b"value");
    /// let value = buffer.get("key_get");
    /// ```
    pub fn get(&self, key: &str) -> Option<Block> {
        // Request read locks on cache lists.
        let t1_read_guard = self.t1.read().unwrap();
        let t2_read_guard = self.t2.read().unwrap();

        if let Some(record) = t1_read_guard.get(key).or(t2_read_guard.get(key)) {
            // Cache hit. Set reference bit to 1.
            *record.reference.write().unwrap() = true;
            return Some(record.block.clone());
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
    /// buffer.write("key", b"value");
    /// buffer.erase("key");
    /// ```
    pub fn erase(&self, key: &str) {
        // Remove the block from the cache and cache directory
        if self.t1.write().unwrap().remove(key)
            .or(self.t2.write().unwrap().remove(key)).is_none() {

            self.b1.lock().unwrap().remove(key)
                .or(self.b2.lock().unwrap().remove(key));
        }

        // Delete the file.
        Block::erase(key);
    }

    /// Returns true if the key-value pair exists, regardless of whether it is cached.
    pub fn exists(&self, key: &str) -> bool {
        Block::exists(key)
    }

    /// Returns true if the key-value pair exists and is cached.
    pub fn is_cached(&self, key: &str) -> bool {
        self.t1.read().unwrap().contains_key(key) || self.t2.read().unwrap().contains_key(key)
    }

    /// Loads the file from storage into the cache.
    fn load(&self, key: &str) -> Option<Block> {
        // Load the file from storage
        let block = Block::load(key)?;
        let record = BufferBlock::new(block.clone());

        // Request write locks on all lists.
        let mut t1_write_guard = self.t1.write().unwrap();
        let mut t2_write_guard = self.t2.write().unwrap();
        let mut b1_guard = self.b1.lock().unwrap();
        let mut b2_guard = self.b2.lock().unwrap();
        let mut p_guard = self.p.lock().unwrap();

        // Another thread may have loaded the block into the cache while this one was waiting for
        // the write locks. If this is the case, return the cached block.
        if let Some(record) = t1_write_guard.get(key).or(t2_write_guard.get(key)) {
            // Cache hit. Set reference bit to 1.
            *record.reference.write().unwrap() = true;
            return Some(record.block.clone());
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
        };

        Some(block)
    }

    /// Removes the approximated least recently used page from the cache.
    fn replace(&self,
               t1: &mut RwLockWriteGuard<OrderedHashMap<String, BufferBlock>>,
               t2: &mut RwLockWriteGuard<OrderedHashMap<String, BufferBlock>>,
               b1: &mut MutexGuard<OrderedHashMap<String, ()>>,
               b2: &mut MutexGuard<OrderedHashMap<String, ()>>,
               p: &MutexGuard<usize>) {
        if t1.len() >= max(1, **p) {
            // T1 is at or above target size. Pop the front of T1.
            let (t1_front_key, t1_front_record) = t1.pop_front().unwrap();

            if !*t1_front_record.reference.read().unwrap()
                && *t1_front_record.block.rc.0.lock().unwrap() == 1 {
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
                && *t2_front_record.block.rc.0.lock().unwrap() == 1 {
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

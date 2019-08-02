extern crate memmap;
extern crate omap;

use std::cmp::{max, min};
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::mem;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard, RwLock, RwLockWriteGuard};

use memmap::MmapMut;

use block::{BLOCK_SIZE, BlockReference, RowMajorBlock};

use self::omap::OrderedHashMap;

/// A wrapper around `BlockReference` to keep track of information used by the cache.
struct CacheBlockReference {
    block: BlockReference,
    reference: RwLock<bool>
}

impl CacheBlockReference {
    fn new(block: BlockReference) -> Self {
        CacheBlockReference {
            block,
            reference: RwLock::new(false)
        }
    }
}

unsafe impl Send for CacheBlockReference {}
unsafe impl Sync for CacheBlockReference {}

/// A buffer that reads and writes to storage and maintains a cache in memory. The `BufferManager`
/// is implemented as a key-value store, where the keys are u64 and the values (blocks) are
/// uniformly sized arrays of bytes. The page replacement policy for the cache is Clock with
/// Adaptive Replacement ([CAR](https://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.105.6057)).
/// The `BufferManager` itself is thread-safe. However, it does not guard against concurrent reads
/// and writes to the same block. This must be handled by an external concurrency control module.
pub struct BufferManager {
    capacity: usize,
    block_ids: RwLock<HashMap<String, Vec<u64>>>,
    block_ctr: Mutex<u64>,
    t1: RwLock<OrderedHashMap<u64, CacheBlockReference>>,
    t2: RwLock<OrderedHashMap<u64, CacheBlockReference>>,
    b1: Mutex<OrderedHashMap<u64, ()>>,
    b2: Mutex<OrderedHashMap<u64, ()>>,
    p: Mutex<usize>,
}

impl BufferManager {
    /// Creates a new `BufferManager` with the specified capacity. The `BufferManager` will hold at
    /// maximum `capacity` blocks in memory.
    pub fn with_capacity(capacity: usize) -> Self {
        let (blocks, block_ctr) = Self::inventory();
        BufferManager {
            capacity,
            block_ids: RwLock::new(blocks),
            block_ctr: Mutex::new(block_ctr),
            t1: RwLock::new(OrderedHashMap::new()),
            t2: RwLock::new(OrderedHashMap::new()),
            b1: Mutex::new(OrderedHashMap::new()),
            b2: Mutex::new(OrderedHashMap::new()),
            p: Mutex::new(0),
        }
    }

    /// Loads the block with the specified `block_id` into the cache and returns a reference if it
    /// exists.
    pub fn get_block(&self, block_group: &str, block_id: u64) -> Option<BlockReference> {
        // Request read locks on cache lists.
        let t1_read_guard = self.t1.read().unwrap();
        let t2_read_guard = self.t2.read().unwrap();

        if let Some(cache_block) = t1_read_guard.get(&block_id).or(t2_read_guard.get(&block_id)) {
            // Cache hit. Set reference bit to 1.
            *cache_block.reference.write().unwrap() = true;
            return Some(cache_block.block.clone());
        }

        // Cache miss. Drop read locks on cache lists.
        mem::drop(t1_read_guard);
        mem::drop(t2_read_guard);

        // Load the file from storage.
        let block = self.load(block_group, block_id);

        // Return the cached block.
        block
    }

    /// Removes the block with the specified `block_id` from the cache and deletes the underlying
    /// file on storage.
    pub fn erase(&self, block_group: &str, block_id: u64) {
        // Remove the block from the cache and cache directory
        if self.t1.write().unwrap().remove(&block_id)
            .or(self.t2.write().unwrap().remove(&block_id)).is_none() {

            self.b1.lock().unwrap().remove(&block_id)
                .or(self.b2.lock().unwrap().remove(&block_id));
        }

        // Delete the file.
        let path = Self::file_path(block_group, block_id);
        fs::remove_file(&path)
            .expect("Error removing file.");
    }

    /// Loads the block from storage into the cache.
    fn load(&self, block_group: &str, block_id: u64) -> Option<BlockReference> {
        // Load the file from storage
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&Self::file_path(block_group, block_id))
            .ok()?;

        file.set_len(BLOCK_SIZE as u64).ok()?;

        let mut mmap = unsafe { MmapMut::map_mut(&file).ok()? };
        let block = BlockReference::new(RowMajorBlock::with_buf(mmap));
        let cache_block = CacheBlockReference::new(block.clone());

        // Request write locks on all lists.
        let mut t1_write_guard = self.t1.write().unwrap();
        let mut t2_write_guard = self.t2.write().unwrap();
        let mut b1_guard = self.b1.lock().unwrap();
        let mut b2_guard = self.b2.lock().unwrap();
        let mut p_guard = self.p.lock().unwrap();

        // Another thread may have loaded the block into the cache while this one was waiting for
        // the write locks. If this is the case, return the cached block.
        if let Some(cache_block) = t1_write_guard.get(&block_id)
            .or(t2_write_guard.get(&block_id))
        {
            // Cache hit. Set reference bit to 1.
            *cache_block.reference.write().unwrap() = true;
            return Some(cache_block.block.clone());
        };

        let cache_directory_miss = !b1_guard.contains_key(&block_id)
            && !b2_guard.contains_key(&block_id);

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
                };
            };
        };

        if cache_directory_miss {
            // Move the page to the back of T1.
            t1_write_guard.push_back(block_id, cache_block);

        } else if b1_guard.contains_key(&block_id) {
            // B1 cache directory hit. Increase the target size for T1.
            *p_guard = min(*p_guard + max(1, b2_guard.len() / b1_guard.len()), self.capacity);

            // Move the page to the back of T2.
            b1_guard.remove(&block_id);
            t2_write_guard.push_back(block_id, cache_block);

        } else {
            // B2 cache directory hit. Decrease the target size for T2.
            *p_guard = max(*p_guard - max(1, b1_guard.len() / b2_guard.len()), 0);

            // Move the page to the back of T2.
            b2_guard.remove(&block_id);
            t2_write_guard.push_back(block_id, cache_block);
        };

        Some(block)
    }

    /// Removes the approximated least recently used page from the cache.
    fn replace(&self,
               t1: &mut RwLockWriteGuard<OrderedHashMap<u64, CacheBlockReference>>,
               t2: &mut RwLockWriteGuard<OrderedHashMap<u64, CacheBlockReference>>,
               b1: &mut MutexGuard<OrderedHashMap<u64, ()>>,
               b2: &mut MutexGuard<OrderedHashMap<u64, ()>>,
               p: &MutexGuard<usize>) {
        if t1.len() >= max(1, **p) {
            // T1 is at or above target size. Pop the front of T1.
            let (t1_front_block_id, t1_front_block) = t1.pop_front().unwrap();

            if !*t1_front_block.reference.read().unwrap()
                && *t1_front_block.block.get_reference_count().0.lock().unwrap() == 1 {
                // Page reference bit is 0 and no other threads are using it.
                // Replace this page and push the block_id to the MRU position of B1.
                b1.push_back(t1_front_block_id, ());
            } else {
                // Set the page reference bit to 0. Push the block to the back of T2.
                *t1_front_block.reference.write().unwrap() = false;
                t2.push_back(t1_front_block_id, t1_front_block);
            }

        } else {
            // Pop the front of T2.
            let (t2_front_block_id, t2_front_block) = t2.pop_front().unwrap();

            if !*t2_front_block.reference.read().unwrap()
                && *t2_front_block.block.get_reference_count().0.lock().unwrap() == 1 {
                // Page reference bit is 0 and no other threads are using it.
                // Replace this page and push the block_id to the MRU position of B2.
                b2.push_back(t2_front_block_id, ());
            } else {
                // Set the page reference bit to 0. Push the block to the back of T2.
                *t2_front_block.reference.write().unwrap() = false;
                t2.push_back(t2_front_block_id, t2_front_block);
            }
        }
    }

    fn inventory() -> (HashMap<String, Vec<u64>>, u64) {
        let mut block_ctr = 0;
        let blocks_dir = Self::blocks_dir();

        if !blocks_dir.exists() {
            fs::create_dir(&blocks_dir);
        }

        let mut blocks = HashMap::new();

        for group_entry in fs::read_dir(blocks_dir).unwrap() {
            let group_entry = group_entry.unwrap();
            let group_path = group_entry.path();
            let block_group = group_entry.file_name()
                .to_str().unwrap()
                .to_owned();

            let mut block_ids = vec![];

            for block_entry in fs::read_dir(group_path).unwrap() {
                let block_id = block_entry.unwrap().file_name()
                    .to_str().unwrap()
                    .parse::<u64>().unwrap();

                if block_id > block_ctr {
                    block_ctr = block_id;
                }

                block_ids.push(block_id)
            }

            blocks.insert(block_group, block_ids);
        }

        (blocks, block_ctr)
    }

    fn file_path(block_group: &str, block_id: u64) -> PathBuf {
        let mut path = Self::blocks_dir();
        path.push(block_group);
        path.push(format!("{}.hsl", block_id));
        path
    }

    fn blocks_dir() -> PathBuf {
        PathBuf::from("blocks")
    }
}

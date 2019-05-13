use std::mem;
use std::rc::Rc;
use std::sync::Mutex;

use memmap::Mmap;

use buffer_manager::BufferManager;
use storage_manager::TEMP_PREFIX;

/// A storage engine for managing key-value pairs. Keys are strings and values are arbitrarily long
/// slices of bytes.
pub struct KeyValueStorageEngine {
    buffer_manager: Rc<BufferManager>,
    anon_ctr: Mutex<u64>
}

impl KeyValueStorageEngine {
    pub fn new(buffer_manager: Rc<BufferManager>) -> Self {
        KeyValueStorageEngine {
            buffer_manager,
            anon_ctr: Mutex::new(0)
        }
    }

    /// Stores the key-value pair.
    pub fn put(&self, key: &str, value: &[u8]) {
        self.buffer_manager.write_uncached(&Self::formatted_key(key), value);
    }

    /// Stores the value, generating and returning a unique key that can be used to request it.
    pub fn put_anon(&self, value: &[u8]) -> String {
        // Generate a unique key, prefixed with the reserved character.
        let mut anon_ctr = self.anon_ctr.lock().unwrap();
        let key = format!("{}{}", TEMP_PREFIX, *anon_ctr);
        *anon_ctr = anon_ctr.wrapping_add(1);
        mem::drop(anon_ctr);

        // Put the value and return the key.
        self.put(key.as_str(), value);
        key
    }

    /// Deletes the key-value pair from storage.
    pub fn delete(&self, key: &str) {
        self.buffer_manager.erase(&Self::formatted_key(key));
    }

    /// Returns the value for the specified `key` as a memory-mapped buffer if it exists.
    pub fn get(&self, key: &str) -> Option<Mmap> {
        self.buffer_manager.get_uncached(&Self::formatted_key(key))
    }

    fn formatted_key(key: &str) -> String {
        format!("{}.kv", key)
    }
}

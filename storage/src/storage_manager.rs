use std::sync::Mutex;

use buffer_manager::BufferManager;

pub const TEMP_PREFIX: char = '$';
const DEFAULT_BUFFER_CAPACITY: usize = 1000;

/// Hustle's storage manager. Manages both unstructured key-value pairs and structured relational
/// data using dedicated engines.
pub struct StorageManager {
    buffer_manager: BufferManager,
    anon_ctr: Mutex<u64>,
}

impl StorageManager {
    pub fn new() -> Self {
        Self::with_buffer_capacity(DEFAULT_BUFFER_CAPACITY)
    }

    pub fn with_buffer_capacity(buffer_capacity: usize) -> Self {
        StorageManager {
            buffer_manager: BufferManager::with_capacity(buffer_capacity),
            anon_ctr: Mutex::new(0),
        }
    }

    pub fn new_block(&self, key: &str) {
    }

    pub fn exists(&self, key: &str) {

    }
}

use key_value_storage_engine::KeyValueStorageEngine;
use relational_storage_engine::RelationalStorageEngine;
use buffer_manager::BufferManager;
use std::sync::Arc;

pub const TEMP_PREFIX: char = '$';
const DEFAULT_BUFFER_CAPACITY: usize = 1000;

/// Hustle's storage manager. Manages both unstructured key-value pairs and structured relational
/// data using dedicated engines.
pub struct StorageManager {
    key_value_engine: KeyValueStorageEngine,
    relational_engine: RelationalStorageEngine,
}

impl StorageManager {
    pub fn new() -> Self {
        Self::with_buffer_capacity(DEFAULT_BUFFER_CAPACITY)
    }

    pub fn with_buffer_capacity(buffer_capacity: usize) -> Self {
        let buffer_manager = Arc::new(BufferManager::with_capacity(buffer_capacity));
        StorageManager {
            key_value_engine: KeyValueStorageEngine::new(buffer_manager.clone()),
            relational_engine: RelationalStorageEngine::new(buffer_manager),
        }
    }

    pub fn relational_engine(&self) -> &RelationalStorageEngine {
        &self.relational_engine
    }

    pub fn key_value_engine(&self) -> &KeyValueStorageEngine {
        &self.key_value_engine
    }
}

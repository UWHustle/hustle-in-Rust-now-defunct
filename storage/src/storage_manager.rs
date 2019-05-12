use key_value_storage_engine::KeyValueStorageEngine;
use relational_storage_engine::RelationalStorageEngine;
use buffer_manager::BufferManager;
use std::rc::Rc;

pub const TEMP_RECORD_PREFIX: char = '$';
const DEFAULT_BUFFER_CAPACITY: usize = 1000;

pub struct StorageManager {
    key_value: KeyValueStorageEngine,
    relational: RelationalStorageEngine,
    buffer_manager: Rc<BufferManager>,
}

impl StorageManager {
    pub fn new() -> Self {
        Self::with_buffer_capacity(DEFAULT_BUFFER_CAPACITY)
    }

    pub fn with_buffer_capacity(buffer_capacity: usize) -> Self {
        let buffer_manager = Rc::new(BufferManager::with_capacity(buffer_capacity));
        StorageManager {
            key_value: KeyValueStorageEngine::new(buffer_manager.clone()),
            relational: RelationalStorageEngine::new(buffer_manager.clone()),
            buffer_manager
        }
    }
}

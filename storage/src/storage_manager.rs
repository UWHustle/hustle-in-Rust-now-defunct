use block::BlockReference;
use buffer_manager::BufferManager;

pub const TEMP_PREFIX: char = '$';
const DEFAULT_BUFFER_CAPACITY: usize = 1000;

/// Hustle's storage manager. Manages both unstructured key-value pairs and structured relational
/// data using dedicated engines.
pub struct StorageManager {
    buffer_manager: BufferManager,
}

impl StorageManager {
    pub fn new() -> Self {
        Self::with_buffer_capacity(DEFAULT_BUFFER_CAPACITY)
    }

    pub fn with_buffer_capacity(buffer_capacity: usize) -> Self {
        StorageManager {
            buffer_manager: BufferManager::with_capacity(buffer_capacity),
        }
    }

    pub fn create(&self, schema: &[usize]) -> BlockReference {
        self.buffer_manager.create(schema)
    }

    pub fn get(&self, block_id: u64) -> Option<BlockReference> {
        self.buffer_manager.get(block_id)
    }

    pub fn erase(&self, block_id: u64) {
        self.buffer_manager.erase(block_id);
    }
}

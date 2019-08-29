use block::BlockReference;
use buffer_manager::BufferManager;
use uuid::Uuid;

const DEFAULT_BUFFER_CAPACITY: usize = 1000;
const DEFAULT_DATA_DIRECTORY: &str = "data";

pub struct StorageManager {
    buffer_manager: BufferManager,
}

impl StorageManager {
    pub fn default() -> Self {
        Self::with_buffer_capacity(DEFAULT_BUFFER_CAPACITY)
    }

    pub fn with_buffer_capacity(buffer_capacity: usize) -> Self {
        StorageManager {
            buffer_manager: BufferManager::with_capacity_and_directory(
                buffer_capacity,
                DEFAULT_DATA_DIRECTORY.to_owned(),
            ),
        }
    }

    pub fn with_data_directory(dir: String) -> Self {
        StorageManager {
            buffer_manager: BufferManager::with_capacity_and_directory(
                DEFAULT_BUFFER_CAPACITY,
                dir,
            )
        }
    }

    pub fn with_unique_data_directory() -> Self {
        Self::with_data_directory(Uuid::new_v4().to_string())
    }

    pub fn create_block(&self, col_sizes: Vec<usize>, n_flags: usize) -> BlockReference {
        self.buffer_manager.create(col_sizes, n_flags)
    }

    pub fn get_block(&self, block_id: u64) -> Option<BlockReference> {
        self.buffer_manager.get(block_id)
    }

    pub fn delete_block(&self, block_id: u64) {
        self.buffer_manager.erase(block_id);
    }

    pub fn clear(&self) {
        self.buffer_manager.clear();
    }
}

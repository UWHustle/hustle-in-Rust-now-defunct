use block::BlockReference;
use buffer_manager::BufferManager;
use uuid::Uuid;

const DEFAULT_BUFFER_CAPACITY: usize = 1000;
const DEFAULT_DATA_DIRECTORY: &str = "data";

/// Hustle's storage manager. The `StorageManager` uses the `BufferManager` to read and write blocks
/// to and from a specified data directory.
pub struct StorageManager {
    buffer_manager: BufferManager,
}

impl StorageManager {
    /// Creates a new `StorageManager` with the default data directory and default buffer capacity.
    pub fn default() -> Self {
        Self::with_buffer_capacity(DEFAULT_BUFFER_CAPACITY)
    }

    /// Creates a new `StorageManager` with the default data directory and specified buffer
    /// capacity.
    pub fn with_buffer_capacity(buffer_capacity: usize) -> Self {
        StorageManager {
            buffer_manager: BufferManager::with_capacity_and_directory(
                buffer_capacity,
                DEFAULT_DATA_DIRECTORY.to_owned(),
            ),
        }
    }

    /// Creates a new `StorageManager` with the specified data directory and default buffer
    /// capacity.
    pub fn with_data_directory(dir: String) -> Self {
        StorageManager {
            buffer_manager: BufferManager::with_capacity_and_directory(
                DEFAULT_BUFFER_CAPACITY,
                dir,
            )
        }
    }

    /// Creates a new `StorageManager` with the default buffer capacity and an automatically
    /// generated UUID data directory. This is useful for temporary databases and testing.
    pub fn with_unique_data_directory() -> Self {
        Self::with_data_directory(Uuid::new_v4().to_string())
    }

    /// Creates a new block with the specified `col_sizes` and the number of bit flags.
    pub fn create_block(&self, col_sizes: Vec<usize>, n_flags: usize) -> BlockReference {
        self.buffer_manager.create(col_sizes, n_flags)
    }

    /// Returns a reference to the block with the specified `block_id`.
    pub fn get_block(&self, block_id: u64) -> Option<BlockReference> {
        self.buffer_manager.get(block_id)
    }

    /// Erases the block with the specified `block_id` from memory and storage.
    pub fn delete_block(&self, block_id: u64) {
        self.buffer_manager.erase(block_id);
    }

    /// Deletes all blocks from memory and storage and removes the data directory from the file
    /// system.
    pub fn clear(&self) {
        self.buffer_manager.clear();
    }
}

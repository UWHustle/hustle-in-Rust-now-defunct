use std::mem;
use std::sync::{Mutex, Arc};

use buffer_manager::BufferManager;
use physical_relation::PhysicalRelation;
use relational_block::RelationalBlock;
use storage_manager::TEMP_PREFIX;

/// A storage engine for managing relational data.
pub struct RelationalStorageEngine {
    buffer_manager: Arc<BufferManager>,
    anon_ctr: Mutex<u64>
}

impl RelationalStorageEngine {
    pub fn new(buffer_manager: Arc<BufferManager>) -> Self {
        RelationalStorageEngine {
            buffer_manager,
            anon_ctr: Mutex::new(0)
        }
    }

    /// Creates a new `PhysicalRelation` with the specified `key` and `schema`.
    pub fn create<'a>(&'a self, key: &'a str, schema: Vec<usize>) -> PhysicalRelation {
        assert!(!self.exists(key), "Relation already exists");
        assert!(!schema.is_empty());
        PhysicalRelation::new(key, schema, self.buffer_manager.clone())
    }

    /// Creates a new `PhysicalRelation` with the specified `schema`, generating a unique `key` that
    /// can be used to access it.
    pub fn create_anon<'a>(&'a self, key: &'a mut String, schema: Vec<usize>) -> PhysicalRelation {
        // Generate a unique key, prefixed with the reserved character.
        let mut anon_ctr = self.anon_ctr.lock().unwrap();
        let generated_key = loop {
            let mut generated_key = format!("{}{}", TEMP_PREFIX, *anon_ctr);
            *anon_ctr = anon_ctr.wrapping_add(1);
            if !self.buffer_manager.exists(&Self::formatted_key_for_block(&generated_key, 0)) {
                break generated_key
            };
        };
        mem::drop(anon_ctr);

        key.clear();
        key.push_str(&generated_key);

        // Create the relation.
        self.create(key.as_str(), schema)
    }

    /// Returns the `PhysicalRelation` associated with `key` if it exists.
    pub fn get<'a>(&'a self, key: &'a str) -> Option<PhysicalRelation> {
        PhysicalRelation::try_from_block(key, self.buffer_manager.clone())
    }

    /// Gets the `RelationalBlock` at the specified `block_index`.
    pub fn get_block<'a>(&'a self, key: &'a str, block_index: usize) -> Option<RelationalBlock> {
        let block_name = RelationalStorageEngine::formatted_key_for_block(key, block_index);
        self.buffer_manager.get(&block_name)
    }

    /// Removes the `PhysicalRelation` from storage.
    pub fn drop(&self, key: &str) {
        let mut block_index = 0;
        let mut key_for_block = Self::formatted_key_for_block(key, block_index);
        while self.buffer_manager.exists(&key_for_block) {
            self.buffer_manager.erase(&key_for_block);
            block_index += 1;
            key_for_block = Self::formatted_key_for_block(key, block_index);
        }
    }

    pub fn copy(&self, from: &str, to: &str) {
        let mut block_index = 0;
        let mut key_for_from_block = Self::formatted_key_for_block(from, block_index);
        let mut key_for_to_block = Self::formatted_key_for_block(to, block_index);
        while self.buffer_manager.exists(&key_for_from_block) {
            self.buffer_manager.copy(&key_for_from_block, &key_for_to_block);
            block_index += 1;
            key_for_from_block = Self::formatted_key_for_block(from, block_index);
            key_for_to_block = Self::formatted_key_for_block(to, block_index);
        }
    }

    /// Returns true if there exists a `PhysicalRelation` with the specified `key`.
    pub fn exists(&self, key: &str) -> bool {
        let key_for_first_block = Self::formatted_key_for_block(key, 0);
        self.buffer_manager.exists(&key_for_first_block)
    }

    pub fn formatted_key_for_block(key: &str, block_index: usize) -> String {
        format!("{}.{}", key, block_index)
    }
}

use buffer_manager::BufferManager;
use physical_relation::PhysicalRelation;
use std::path::PathBuf;
use std::rc::Rc;

pub struct RelationalStorageEngine {
    buffer_manager: Rc<BufferManager>
}

impl RelationalStorageEngine {
    pub fn new(buffer_manager: Rc<BufferManager>) -> Self {
        RelationalStorageEngine {
            buffer_manager
        }
    }

    pub fn create<'a>(&'a self, key: &'a str, schema: Vec<usize>) -> PhysicalRelation {
        assert!(!self.exists(key), "Relation already exists");
        assert!(!schema.is_empty());
        PhysicalRelation::new(key, schema, self.buffer_manager.clone())
    }

    pub fn get<'a>(&'a self, key: &'a str) -> Option<PhysicalRelation> {
        let key_for_first_block = Self::key_for_block(key, 0);
        self.buffer_manager.get(&key_for_first_block)
            .map(|block| {
                let mut schema = vec![];
                schema.extend_from_slice(block.get_schema());
                PhysicalRelation::new(key, schema, self.buffer_manager.clone())
            })
    }

    pub fn drop(&self, key: &str) {
        let mut block_index = 0;
        let mut key_for_block = Self::key_for_block(key, block_index);
        while self.buffer_manager.exists(&key_for_block) {
            self.buffer_manager.erase(&key_for_block);
            block_index += 1;
            key_for_block = Self::key_for_block(key, block_index);
        }
    }

    pub fn exists(&self, key: &str) -> bool {
        let key_for_first_block = Self::key_for_block(key, 0);
        self.buffer_manager.exists(&key_for_first_block)
    }

    pub fn key_for_block(key: &str, block_index: usize) -> String {
        format!("{}.{}", key, block_index)
    }
}

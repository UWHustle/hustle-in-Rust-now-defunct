use std::mem;
use std::rc::Rc;
use std::sync::Mutex;

use buffer_manager::BufferManager;
use physical_relation::PhysicalRelation;
use storage_manager::TEMP_PREFIX;

pub struct RelationalStorageEngine {
    buffer_manager: Rc<BufferManager>,
    anon_ctr: Mutex<u64>
}

impl RelationalStorageEngine {
    pub fn new(buffer_manager: Rc<BufferManager>) -> Self {
        RelationalStorageEngine {
            buffer_manager,
            anon_ctr: Mutex::new(0)
        }
    }

    pub fn create<'a>(&'a self, key: &'a str, schema: Vec<usize>) -> PhysicalRelation {
        assert!(!self.exists(key), "Relation already exists");
        assert!(!schema.is_empty());
        PhysicalRelation::new(key, schema, self.buffer_manager.clone())
    }

    pub fn create_anon<'a>(&'a self, key: &'a mut String, schema: Vec<usize>) -> PhysicalRelation {
        // Generate a unique key, prefixed with the reserved character.
        let mut anon_ctr = self.anon_ctr.lock().unwrap();
        let generated_key = format!("{}{}", TEMP_PREFIX, *anon_ctr);
        *anon_ctr = anon_ctr.wrapping_add(1);
        mem::drop(anon_ctr);

        key.clear();
        key.push_str(&generated_key);

        // Create the relation.
        self.create(key, schema)
    }

    pub fn get<'a>(&'a self, key: &'a str) -> Option<PhysicalRelation> {
        PhysicalRelation::try_from_block(key, self.buffer_manager.clone())
    }

    pub fn drop(&self, key: &str) {
        let mut block_index = 0;
        let mut key_for_block = Self::formatted_key_for_block(key, block_index);
        while self.buffer_manager.exists(&key_for_block) {
            self.buffer_manager.erase(&key_for_block);
            block_index += 1;
            key_for_block = Self::formatted_key_for_block(key, block_index);
        }
    }

    pub fn exists(&self, key: &str) -> bool {
        let key_for_first_block = Self::formatted_key_for_block(key, 0);
        self.buffer_manager.exists(&key_for_first_block)
    }

    pub fn formatted_key_for_block(key: &str, block_index: usize) -> String {
        format!("{}.{}", key, block_index)
    }
}

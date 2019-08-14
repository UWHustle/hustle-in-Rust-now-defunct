use std::sync::{Arc, Mutex};
use hustle_storage::StorageManager;
use hustle_storage::block::BlockReference;
use std::ops::Deref;

pub struct BlockPoolBlockReference {
    block: BlockReference,
    available_blocks: Arc<Mutex<Vec<BlockReference>>>,
}

impl BlockPoolBlockReference {
    fn new(
        block: BlockReference,
        available_blocks: Arc<Mutex<Vec<BlockReference>>>,
    ) -> Self {
        BlockPoolBlockReference {
            block,
            available_blocks,
        }
    }
}

impl Deref for BlockPoolBlockReference {
    type Target = BlockReference;

    fn deref(&self) -> &Self::Target {
        &self.block
    }
}

impl Drop for BlockPoolBlockReference {
    fn drop(&mut self) {
        if !self.block.is_full() {
            self.available_blocks.lock().unwrap().push(self.block.clone())
        }
    }
}

pub struct BlockPoolDestinationRouter {
    block_ids: Mutex<Vec<u64>>,
    available_blocks: Arc<Mutex<Vec<BlockReference>>>,
    schema: Vec<usize>,
}

impl BlockPoolDestinationRouter {
    pub fn new(schema: Vec<usize>) -> Self {
        Self::with_block_ids(vec![], schema)
    }

    pub fn with_block_ids(block_ids: Vec<u64>, schema: Vec<usize>) -> Self {
        BlockPoolDestinationRouter {
            block_ids: Mutex::new(block_ids),
            available_blocks: Arc::new(Mutex::new(vec![])),
            schema,
        }
    }

    pub fn get_block(
        &self,
        storage_manager: &StorageManager,
    ) -> BlockPoolBlockReference {
        let block = self.available_blocks.lock().unwrap().pop()
            .or_else(|| {
                loop {
                    if let Some(block_id) = self.block_ids.lock().unwrap().pop() {
                        let block = storage_manager.get_block(block_id).unwrap();
                        if !block.is_full() {
                            break Some(block)
                        }
                    } else {
                        break None
                    }
                }
            })
            .unwrap_or(storage_manager.create_block(&self.schema));
        BlockPoolBlockReference::new(block, self.available_blocks.clone())
    }
}

use block::BlockReference;
use std::sync::{Arc, Mutex};
use StorageManager;

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

impl Drop for BlockPoolBlockReference {
    fn drop(&mut self) {
        if !self.block.is_full() {
            self.available_blocks.lock().unwrap().push(self.block.clone())
        }
    }
}

pub struct BlockPoolDestinationRouter {
    storage_manager: StorageManager,
    schema: Vec<usize>,
    block_ids: Mutex<Vec<u64>>,
    available_blocks: Arc<Mutex<Vec<BlockReference>>>,
}

impl BlockPoolDestinationRouter {
    pub fn new(storage_manager: StorageManager, schema: Vec<usize>, block_ids: Vec<u64>) -> Self {
        BlockPoolDestinationRouter {
            storage_manager,
            schema,
            block_ids: Mutex::new(block_ids),
            available_blocks: Arc::new(Mutex::new(vec![])),
        }
    }

    pub fn get_block(&self) -> BlockPoolBlockReference {
        let block = self.available_blocks.lock().unwrap().pop()
            .or_else(|| {
                loop {
                    if let Some(block_id) = self.block_ids.lock().unwrap().pop() {
                        let block = self.storage_manager.get(block_id).unwrap();
                        if !block.is_full() {
                            break Some(block)
                        }
                    } else {
                        break None
                    }
                }
            })
            .unwrap_or(self.storage_manager.create(&self.schema));
        BlockPoolBlockReference::new(block, self.available_blocks.clone())
    }
}

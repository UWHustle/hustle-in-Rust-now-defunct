use std::sync::Mutex;

use hustle_catalog::Column;
use hustle_storage::block::BlockReference;
use hustle_storage::StorageManager;

pub struct BlockPoolDestinationRouter {
    available_block_ids: Mutex<Vec<u64>>,
    created_block_ids: Mutex<Vec<u64>>,
    unfilled_blocks: Mutex<Vec<BlockReference>>,
    col_sizes: Vec<usize>,
}

impl BlockPoolDestinationRouter {
    pub fn new(schema: Vec<Column>) -> Self {
        Self::with_block_ids(vec![], schema)
    }

    pub fn with_block_ids(block_ids: Vec<u64>, schema: Vec<Column>) -> Self {
        let col_sizes = schema.into_iter()
            .map(|c| c.into_type_variant().into_type().byte_len())
            .collect();

        BlockPoolDestinationRouter {
            available_block_ids: Mutex::new(block_ids),
            created_block_ids: Mutex::new(vec![]),
            unfilled_blocks: Mutex::new(vec![]),
            col_sizes,
        }
    }

    pub fn get_block(&self, storage_manager: &StorageManager) -> BlockReference {
        self.unfilled_blocks.lock().unwrap().pop()
            .or(
                self.available_block_ids.lock().unwrap().pop()
                    .and_then(|block_id| storage_manager.get_block(block_id))
            )
            .unwrap_or_else(|| {
                let block = storage_manager.create_block(self.col_sizes.clone(), 0);
                self.created_block_ids.lock().unwrap().push(block.id);
                block
            })
    }

    pub fn return_block(&self, block: BlockReference) {
        self.unfilled_blocks.lock().unwrap().push(block);
    }

    pub fn get_created_block_ids(&self) -> Vec<u64> {
        self.created_block_ids.lock().unwrap().clone()
    }

    pub fn get_all_block_ids(&self) -> Vec<u64> {
        let mut block_ids = self.available_block_ids.lock().unwrap().clone();
        block_ids.extend(self.unfilled_blocks.lock().unwrap().iter().map(|block| block.id));
        block_ids
    }
}

use std::sync::Mutex;

use hustle_catalog::Column;
use hustle_storage::block::BlockReference;
use hustle_storage::StorageManager;
use std::sync::mpsc::Sender;

/// A router that manages destination blocks for insert operations. The aim of the
/// `BlockPoolDestinationRouter` is to provide each thread with sole access to a block in order to
/// minimize lock contention, while maintaining sufficient packing in the blocks.
pub struct BlockPoolDestinationRouter {
    available_block_ids: Mutex<Vec<u64>>,
    created_block_ids: Mutex<Vec<u64>>,
    unfilled_blocks: Mutex<Vec<BlockReference>>,
    col_sizes: Vec<usize>,
    block_tx: Option<Sender<u64>>,
}

impl BlockPoolDestinationRouter {
    /// Returns a new `BlockPoolDestinationRouter` with the specified `schema`. This should be used
    /// to construct a `BlockPoolDestinationRouter` for a query result.
    pub fn new(schema: Vec<Column>, block_tx: Option<Sender<u64>>) -> Self {
        Self::with_block_ids(vec![], schema, block_tx)
    }

    /// Returns a new `BlockPoolDestinationRouter` with the specified `schema` and `block_ids`. This
    /// should be used to construct a `BlockPoolDestinationRouter` for an insert into an existing
    /// table.
    pub fn with_block_ids(block_ids: Vec<u64>, schema: Vec<Column>, block_tx: Option<Sender<u64>>) -> Self {
        let col_sizes = schema.into_iter()
            .map(|c| c.into_type_variant().into_type().byte_len())
            .collect();

        BlockPoolDestinationRouter {
            available_block_ids: Mutex::new(block_ids),
            created_block_ids: Mutex::new(vec![]),
            unfilled_blocks: Mutex::new(vec![]),
            col_sizes,
            block_tx
        }
    }

    /// Returns a `BlockReference` into which rows can be inserted.
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

    /// Returns the `block` to the `BlockPoolDestinationRouter`.
    pub fn return_block(&self, block: BlockReference) {
        self.unfilled_blocks.lock().unwrap().push(block);
    }

    /// Returns the IDs of all the blocks that have been created by the
    /// `BlockPoolDestinationRouter`. This is used to update the catalog after insert operations.
    pub fn get_created_block_ids(&self) -> Vec<u64> {
        self.created_block_ids.lock().unwrap().clone()
    }

    /// Returns the IDs of all blocks in the `BlockPoolDestinationRouter` that have not been
    /// requested.
    pub fn get_all_block_ids(&self) -> Vec<u64> {
        let mut block_ids = self.available_block_ids.lock().unwrap().clone();
        block_ids.extend(self.unfilled_blocks.lock().unwrap().iter().map(|block| block.id));
        block_ids
    }
}

impl Drop for BlockPoolDestinationRouter {
    fn drop(&mut self) {
        if let Some(block_tx) = &self.block_tx {
            for block_id in self.get_all_block_ids() {
                block_tx.send(block_id).unwrap()
            }
        }
    }
}

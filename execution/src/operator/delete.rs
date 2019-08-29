use hustle_storage::block::{BlockReference, RowMask};
use crate::operator::Operator;
use hustle_storage::StorageManager;
use hustle_catalog::Catalog;

pub struct Delete {
    filter: Option<Box<dyn Fn(&BlockReference) -> RowMask>>,
    block_ids: Vec<u64>,
}

impl Delete {
    pub fn new(
        filter: Option<Box<dyn Fn(&BlockReference) -> RowMask>>,
        block_ids: Vec<u64>,
    ) -> Self {
        Delete {
            filter,
            block_ids,
        }
    }
}

impl Operator for Delete {
    fn execute(&self, storage_manager: &StorageManager, _catalog: &Catalog) {
        for &block_id in &self.block_ids {
            let block = storage_manager.get_block(block_id).unwrap();
            if let Some(filter) = &self.filter {
                let mask = (filter)(&block);
                block.delete_rows_with_mask(&mask);
            } else {
                block.delete_rows();
            }
        }
    }
}

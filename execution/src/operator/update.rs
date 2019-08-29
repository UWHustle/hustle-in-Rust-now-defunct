use hustle_catalog::Catalog;
use hustle_storage::block::{BlockReference, RowMask};
use hustle_storage::StorageManager;

use crate::operator::Operator;

pub struct Update {
    cols: Vec<usize>,
    assignments: Vec<Vec<u8>>,
    filter: Option<Box<dyn Fn(&BlockReference) -> RowMask>>,
    block_ids: Vec<u64>,
}

impl Update {
    pub fn new(
        cols: Vec<usize>,
        assignments: Vec<Vec<u8>>,
        filter: Option<Box<dyn Fn(&BlockReference) -> RowMask>>,
        block_ids: Vec<u64>,
    ) -> Self {
        Update {
            cols,
            assignments,
            filter,
            block_ids,
        }
    }
}

impl Operator for Update {
    fn execute(&self, storage_manager: &StorageManager, _catalog: &Catalog) {
        for &block_id in &self.block_ids {
            let block = storage_manager.get_block(block_id).unwrap();
            if let Some(filter) = &self.filter {
                let mask = (filter)(&block);
                for (&col_i, assignment) in self.cols.iter().zip(&self.assignments) {
                    block.update_col_with_mask(col_i, assignment, &mask);
                }
            } else {
                for (&col_i, assignment) in self.cols.iter().zip(&self.assignments) {
                    block.update_col(col_i, assignment);
                }
            }
        }
    }
}

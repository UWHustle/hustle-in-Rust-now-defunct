use hustle_catalog::Catalog;
use hustle_storage::block::{BlockReference, RowMask};
use hustle_storage::StorageManager;

use crate::operator::Operator;

pub struct Update {
    assignments: Vec<(usize, Vec<u8>)>,
    filter: Option<Box<dyn Fn(&BlockReference) -> RowMask>>,
    block_ids: Vec<u64>,
}

impl Update {
    pub fn new(
        assignments: Vec<(usize, Vec<u8>)>,
        filter: Option<Box<dyn Fn(&BlockReference) -> RowMask>>,
        block_ids: Vec<u64>,
    ) -> Self {
        Update {
            assignments,
            filter,
            block_ids,
        }
    }
}

impl Operator for Update {
    fn execute(self: Box<Self>, storage_manager: &StorageManager, _catalog: &Catalog) {
        for &block_id in &self.block_ids {
            let block = storage_manager.get_block(block_id).unwrap();
            if let Some(filter) = &self.filter {
                let mask = (filter)(&block);
                for (col_i, assignment) in &self.assignments {
                    block.update_col_with_mask(
                        *col_i,
                        assignment,
                        &mask,
                        |_row_i, _buf| () // TODO: Write the row ID and old value to storage.
                    );
                }
            } else {
                for (col_i, assignment) in &self.assignments {
                    block.update_col(
                        *col_i,
                        assignment,
                        |_row_i, _buf| () // TODO: Write the row ID and old value to storage.
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod update_tests {
    use hustle_execution_test_util as test_util;
    use hustle_types::{Bool, HustleType, Int64};

    use super::*;

    #[test]
    fn update() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let catalog = Catalog::new();
        let block = test_util::example_block(&storage_manager);

        let old_values = block.project(&[0, 1, 2])
            .map(|row| row.map(|buf| buf.to_vec()).collect::<Vec<Vec<u8>>>())
            .collect::<Vec<Vec<Vec<u8>>>>();

        let int64_type = Int64;
        let mut buf = vec![0; int64_type.byte_len()];
        int64_type.set(2, &mut buf);

        let filter = Box::new(|block: &BlockReference|
            block.filter_col(0, |buf| Bool.get(buf))
        );

        let update = Box::new(Update::new(vec![(1, buf.clone())], Some(filter), vec![block.id]));
        update.execute(&storage_manager, &catalog);

        assert_eq!(block.get_row_col(0, 0), Some(old_values[0][0].as_slice()));
        assert_eq!(block.get_row_col(0, 1), Some(old_values[0][1].as_slice()));
        assert_eq!(block.get_row_col(0, 2), Some(old_values[0][2].as_slice()));
        assert_eq!(block.get_row_col(1, 0), Some(old_values[1][0].as_slice()));
        assert_eq!(block.get_row_col(1, 1), Some(buf.as_slice()));
        assert_eq!(block.get_row_col(1, 2), Some(old_values[1][2].as_slice()));

        storage_manager.clear();
    }
}

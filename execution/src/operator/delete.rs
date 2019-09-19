use hustle_catalog::Catalog;
use hustle_storage::{LogManager, StorageManager};
use hustle_storage::block::{BlockReference, RowMask};

use crate::operator::Operator;

pub struct Delete {
    filter: Option<Box<dyn Fn(&BlockReference) -> RowMask>>,
    block_ids: Vec<u64>,
    transaction_id: u64,
}

impl Delete {
    pub fn new(
        filter: Option<Box<dyn Fn(&BlockReference) -> RowMask>>,
        block_ids: Vec<u64>,
        transaction_id: u64,
    ) -> Self {
        Delete {
            filter,
            block_ids,
            transaction_id,
        }
    }
}

impl Operator for Delete {
    fn execute(
        self: Box<Self>,
        storage_manager: &StorageManager,
        log_manager: &LogManager,
        _catalog: &Catalog
    ) {
        for &block_id in &self.block_ids {
            let block = storage_manager.get_block(block_id).unwrap();
            if let Some(filter) = &self.filter {
                let mask = (filter)(&block);
                block.tentative_delete_rows_with_mask(
                    &mask,
                    |row_id| log_manager.log_delete(
                        self.transaction_id,
                        block.id,
                        row_id as u64,
                    )
                );
            } else {
                block.tentative_delete_rows(|row_id| log_manager.log_delete(
                    self.transaction_id,
                    block.id,
                    row_id as u64,
                ));
            }
        }
    }
}

#[cfg(test)]
mod delete_tests {
    use hustle_execution_test_util as test_util;
    use hustle_types::Bool;

    use super::*;

    #[test]
    fn delete() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let log_manager = LogManager::with_unique_log_directory();
        let catalog = Catalog::new();
        let block = test_util::example_block(&storage_manager);

        let delete = Box::new(Delete::new(None, vec![block.id], 0));
        delete.execute(&storage_manager, &log_manager, &catalog);

        assert!(block.project(&[0, 1, 2]).next().is_none());

        storage_manager.clear();
    }

    #[test]
    fn delete_with_filter() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let log_manager = LogManager::with_unique_log_directory();
        let catalog = Catalog::new();
        let block = test_util::example_block(&storage_manager);

        let filter = Box::new(|block: &BlockReference|
            block.filter_col(0, |buf| Bool.get(buf))
        );

        let delete = Box::new(Delete::new(Some(filter), vec![block.id], 0));
        delete.execute(&storage_manager, &log_manager, &catalog);

        assert!(block.project(&[0, 1, 2]).next().is_some());
        assert_eq!(block.get_row_col(1, 0), None);

        storage_manager.clear();
        log_manager.clear();
    }
}

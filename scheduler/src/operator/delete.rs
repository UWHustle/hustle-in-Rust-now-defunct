use hustle_storage::block::{BlockReference, RowMask};
use crate::operator::{Operator, WorkOrder};
use hustle_storage::StorageManager;
use std::sync::Arc;
use std::sync::mpsc::Sender;

pub struct Delete {
    storage_manager: Arc<StorageManager>,
    filter: Arc<Option<Box<dyn Fn(&BlockReference) -> RowMask>>>,
    block_ids: Vec<u64>,
    work_order_tx: Sender<Box<dyn WorkOrder>>
}

impl Delete {
    pub fn new(
        storage_manager: Arc<StorageManager>,
        filter: Option<Box<dyn Fn(&BlockReference) -> RowMask>>,
        block_ids: Vec<u64>,
        work_order_tx: Sender<Box<dyn WorkOrder>>
    ) -> Self {
        Delete {
            storage_manager,
            filter: Arc::new(filter),
            block_ids,
            work_order_tx
        }
    }
}

unsafe impl Send for Delete {}

impl Operator for Delete {
    fn push_work_orders(&self) {
        for &block_id in self.block_ids.iter() {
            let work_order = Box::new(DeleteWorkOrder {
                storage_manager: self.storage_manager.clone(),
                filter: self.filter.clone(),
                block_id
            });
            //work_orders.lock().unwrap().push_back(work_order);
            self.work_order_tx.send(work_order).unwrap();
        }
    }
}

struct DeleteWorkOrder {
    storage_manager: Arc<StorageManager>,
    filter: Arc<Option<Box<dyn Fn(&BlockReference) -> RowMask>>>,
    block_id: u64,
}

unsafe impl Send for DeleteWorkOrder {}

impl WorkOrder for DeleteWorkOrder {
    fn execute(&self) {
        let block = self.storage_manager.get_block(self.block_id).unwrap();
        if let Some(filter) = &*self.filter {
            let mask = (filter)(&block);
            block.delete_rows_with_mask(&mask);
        } else {
            block.delete_rows();
        }
    }
}


//#[cfg(test)]
//mod delete_tests {
//    use hustle_execution_test_util as test_util;
//    use super::*;
//    use hustle_types::Bool;
//
//    #[test]
//    fn delete() {
//        let storage_manager = StorageManager::with_unique_data_directory();
//        let catalog = Catalog::new();
//        let block = test_util::example_block(&storage_manager);
//
//        let delete = Box::new(Delete::new(None, vec![block.id]));
//        delete.execute(&storage_manager, &catalog);
//
//        assert!(block.project(&[0, 1, 2]).next().is_none());
//
//        storage_manager.clear();
//    }
//
//    #[test]
//    fn delete_with_filter() {
//        let storage_manager = StorageManager::with_unique_data_directory();
//        let catalog = Catalog::new();
//        let block = test_util::example_block(&storage_manager);
//
//        let filter = Box::new(|block: &BlockReference|
//            block.filter_col(0, |buf| Bool.get(buf))
//        );
//
//        let delete = Box::new(Delete::new(Some(filter), vec![block.id]));
//        delete.execute(&storage_manager, &catalog);
//
//        assert!(block.project(&[0, 1, 2]).next().is_some());
//        assert_eq!(block.get_row_col(1, 0), None);
//
//        storage_manager.clear();
//    }
//}

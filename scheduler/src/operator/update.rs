use hustle_storage::block::{BlockReference, RowMask};
use hustle_storage::StorageManager;

use crate::operator::{Operator, WorkOrder};
use std::sync::Arc;
use std::sync::mpsc::Sender;

pub struct Update {
    storage_manager: Arc<StorageManager>,
    assignments: Arc<Vec<(usize, Vec<u8>)>>,
    filter: Arc<Option<Box<dyn Fn(&BlockReference) -> RowMask>>>,
    block_ids: Vec<u64>,
    work_order_tx: Sender<Box<dyn WorkOrder>>
}

impl Update {
    pub fn new(
        storage_manager: Arc<StorageManager>,
        assignments: Vec<(usize, Vec<u8>)>,
        filter: Option<Box<dyn Fn(&BlockReference) -> RowMask>>,
        block_ids: Vec<u64>,
        work_order_tx: Sender<Box<dyn WorkOrder>>
    ) -> Self {
        Update {
            storage_manager,
            assignments: Arc::new(assignments),
            filter: Arc::new(filter),
            block_ids,
            work_order_tx
        }
    }
}

unsafe impl Send for Update {}

impl Operator for Update {
    fn push_work_orders(&self) {
        for &block_id in self.block_ids.iter() {
            let work_order = Box::new(UpdateWorkOrder {
                storage_manager: self.storage_manager.clone(),
                assignments: self.assignments.clone(),
                filter: self.filter.clone(),
                block_id
            });
            //work_orders.lock().unwrap().push_back(work_order);
            self.work_order_tx.send(work_order).unwrap();
        }
    }
}

pub struct UpdateWorkOrder {
    storage_manager: Arc<StorageManager>,
    assignments: Arc<Vec<(usize, Vec<u8>)>>,
    filter: Arc<Option<Box<dyn Fn(&BlockReference) -> RowMask>>>,
    block_id: u64
}

unsafe impl Send for UpdateWorkOrder {}

impl WorkOrder for UpdateWorkOrder {
    fn execute(&self) {
        let block = self.storage_manager.get_block(self.block_id).unwrap();
        if let Some(filter) = &*self.filter {
            let mask = (filter)(&block);
            for (col_i, assignment) in &*self.assignments {
                block.update_col_with_mask(*col_i, assignment, &mask);
            }
        } else {
            for (col_i, assignment) in &*self.assignments {
                block.update_col(*col_i, assignment);
            }
        }
    }
}



//#[cfg(test)]
//mod update_tests {
//    use hustle_execution_test_util as test_util;
//    use hustle_types::{Bool, HustleType, Int64};
//
//    use super::*;
//
//    #[test]
//    fn update() {
//        let storage_manager = StorageManager::with_unique_data_directory();
//        let catalog = Catalog::new();
//        let block = test_util::example_block(&storage_manager);
//
//        let old_values = block.project(&[0, 1, 2])
//            .map(|row| row.map(|buf| buf.to_vec()).collect::<Vec<Vec<u8>>>())
//            .collect::<Vec<Vec<Vec<u8>>>>();
//
//        let int64_type = Int64;
//        let mut buf = vec![0; int64_type.byte_len()];
//        int64_type.set(2, &mut buf);
//
//        let filter = Box::new(|block: &BlockReference|
//            block.filter_col(0, |buf| Bool.get(buf))
//        );
//
//        let update = Box::new(Update::new(vec![(1, buf.clone())], Some(filter), vec![block.id]));
//        update.execute(&storage_manager, &catalog);
//
//        assert_eq!(block.get_row_col(0, 0), Some(old_values[0][0].as_slice()));
//        assert_eq!(block.get_row_col(0, 1), Some(old_values[0][1].as_slice()));
//        assert_eq!(block.get_row_col(0, 2), Some(old_values[0][2].as_slice()));
//        assert_eq!(block.get_row_col(1, 0), Some(old_values[1][0].as_slice()));
//        assert_eq!(block.get_row_col(1, 1), Some(buf.as_slice()));
//        assert_eq!(block.get_row_col(1, 2), Some(old_values[1][2].as_slice()));
//
//        storage_manager.clear();
//    }
//}

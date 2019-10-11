
use hustle_storage::block::{BlockReference, RowMask};
use hustle_storage::StorageManager;

use crate::operator::{Operator, util, WorkOrder};
use crate::router::BlockPoolDestinationRouter;

use std::sync::Arc;
use std::sync::mpsc::{Sender, Receiver};

pub struct Select {
    storage_manager: Arc<StorageManager>,
    filter: Arc<Box<dyn Fn(&BlockReference) -> RowMask>>,
    router: Arc<BlockPoolDestinationRouter>,
    block_rx: Receiver<u64>,
    block_tx: Sender<u64>,
    work_order_tx: Sender<Box<dyn WorkOrder>>,
}

impl Select {
    pub fn new(
        storage_manager: Arc<StorageManager>,
        filter: Box<dyn Fn(&BlockReference) -> RowMask>,
        router: Arc<BlockPoolDestinationRouter>,
        block_rx: Receiver<u64>,
        block_tx: Sender<u64>,
        work_order_tx: Sender<Box<dyn WorkOrder>>,
    ) -> Self {
        Select {
            storage_manager,
            filter: Arc::new(filter),
            router,
            block_rx,
            block_tx,
            work_order_tx,
        }
    }
}

unsafe impl Send for Select {}

impl Operator for Select {
    fn push_work_orders(&self) {
        println!("Select push_work_orders entered");
        for block_id in &self.block_rx {
            let work_order = Box::new(SelectWorkOrder {
                storage_manager: self.storage_manager.clone(),
                filter: self.filter.clone(),
                router: self.router.clone(),
                block_tx: self.block_tx.clone(),
                block_id
            });
            //work_orders.lock().unwrap().push_back(work_order);
            self.work_order_tx.send(work_order).unwrap();
            println!("SelectWorkOrder sent, block_id: {}", block_id);
        }
        println!("Select push_work_orders left");
    }
}

struct SelectWorkOrder {
    storage_manager: Arc<StorageManager>,
    filter: Arc<Box<dyn Fn(&BlockReference) -> RowMask>>,
    router: Arc<BlockPoolDestinationRouter>,
    block_tx: Sender<u64>,
    block_id: u64,
}

unsafe impl Send for SelectWorkOrder {}

impl WorkOrder for SelectWorkOrder {
    fn execute(&self) {
        let input_block = self.storage_manager.get_block(self.block_id).unwrap();
        let mask = (self.filter)(&input_block);
        let mut rows = input_block.rows_with_mask(&mask);
        util::send_rows(
            &self.storage_manager,
            &mut rows,
            &self.block_tx,
            &self.router,
        );
        println!("SelectWorkOrder finished, block_id: {}", self.block_id);
    }
}


//#[cfg(test)]
//mod select_tests {
//    use std::mem;
//    use std::sync::mpsc;
//
//    use hustle_execution_test_util as test_util;
//    use hustle_types::Bool;
//
//    use crate::router::BlockPoolDestinationRouter;
//
//    use super::*;
//
//    #[test]
//    fn select() {
//        let storage_manager = StorageManager::with_unique_data_directory();
//        let catalog = Catalog::new();
//        let table = test_util::example_table();
//        let input_block = test_util::example_block(&storage_manager);
//        let router = BlockPoolDestinationRouter::new(table.columns);
//
//        let (input_block_tx, input_block_rx) = mpsc::channel();
//        let (output_block_tx, output_block_rx) = mpsc::channel();
//
//        input_block_tx.send(input_block.id).unwrap();
//        mem::drop(input_block_tx);
//
//        let filter = Box::new(|block: &BlockReference|
//            block.filter_col(0, |buf| Bool.get(buf))
//        );
//
//        let select = Box::new(Select::new(filter, router, input_block_rx, output_block_tx));
//        select.execute(&storage_manager, &catalog);
//
//        let output_block = storage_manager.get_block(output_block_rx.recv().unwrap()).unwrap();
//
//        assert_eq!(output_block.get_row_col(0, 0), input_block.get_row_col(1, 0));
//        assert_eq!(output_block.get_row_col(0, 1), input_block.get_row_col(1, 1));
//        assert_eq!(output_block.get_row_col(0, 2), input_block.get_row_col(1, 2));
//        assert_eq!(output_block.get_row_col(0, 3), None);
//
//        storage_manager.clear();
//    }
//}


use hustle_storage::StorageManager;

use crate::operator::{Operator, util, WorkOrder};
use crate::router::BlockPoolDestinationRouter;

use std::sync::Arc;
use std::sync::mpsc::{Sender, Receiver};


pub struct Project {
    storage_manager: Arc<StorageManager>,
    cols: Arc<Vec<usize>>,
    router: Arc<BlockPoolDestinationRouter>,
    block_rx: Receiver<u64>,
    block_tx: Sender<u64>,
    work_order_tx: Sender<Box<dyn WorkOrder>>,
}

impl Project {
    pub fn new(
        storage_manager: Arc<StorageManager>,
        cols: Vec<usize>,
        router: Arc<BlockPoolDestinationRouter>,
        block_rx: Receiver<u64>,
        block_tx: Sender<u64>,
        work_order_tx: Sender<Box<dyn WorkOrder>>,
    ) -> Self {
        Project {
            storage_manager,
            cols: Arc::new(cols),
            router,
            block_rx,
            block_tx,
            work_order_tx
        }
    }
}

unsafe impl Send for Project {}

impl Operator for Project {
    fn push_work_orders(&self) {
        println!("Project push_work_orders entered");
        for block_id in &self.block_rx {
            let work_order = Box::new(ProjectWorkOrder {
                storage_manager: self.storage_manager.clone(),
                cols: self.cols.clone(),
                router: self.router.clone(),
                block_tx: self.block_tx.clone(),
                block_id,
            });
            //work_orders.lock().unwrap().push_back(work_order);
            self.work_order_tx.send(work_order).unwrap();
            println!("ProjectWorkOrder sent, block_id: {}", block_id);
        }
        println!("Project push_work_orders left");
    }
}

struct ProjectWorkOrder {
    storage_manager: Arc<StorageManager>,
    cols: Arc<Vec<usize>>,
    router: Arc<BlockPoolDestinationRouter>,
    block_tx: Sender<u64>,
    block_id: u64,
}

unsafe impl Send for ProjectWorkOrder {}

impl WorkOrder for ProjectWorkOrder {
    fn execute(&self) {
        let input_block = self.storage_manager.get_block(self.block_id).unwrap();
        let mut rows = input_block.project(&self.cols);
        util::send_rows(
            &self.storage_manager,
            &mut rows,
            &self.block_tx,
            &self.router,
        );
        println!("ProjectWorkOrder finished, block_id: {}", self.block_id);
    }
}


//#[cfg(test)]
//mod project_tests {
//    use std::mem;
//    use std::sync::mpsc;
//
//    use hustle_catalog::{Column, Table};
//    use hustle_execution_test_util as test_util;
//    use hustle_types::{Int64, TypeVariant};
//
//    use super::*;
//
//    #[test]
//    fn project() {
//        let storage_manager = StorageManager::with_unique_data_directory();
//        let catalog = Catalog::new();
//        let project_table = Table::new(
//            "project".to_owned(),
//            vec![Column::new(
//                "col_int64".to_owned(),
//                "project".to_owned(),
//                TypeVariant::Int64(Int64),
//                false,
//            )],
//        );
//        let input_block = test_util::example_block(&storage_manager);
//        let router = BlockPoolDestinationRouter::new(project_table.columns);
//
//        let (input_block_tx, input_block_rx) = mpsc::channel();
//        let (output_block_tx, output_block_rx) = mpsc::channel();
//
//        input_block_tx.send(input_block.id).unwrap();
//        mem::drop(input_block_tx);
//
//        let project = Box::new(Project::new(vec![1], router, input_block_rx, output_block_tx));
//        project.execute(&storage_manager, &catalog);
//
//        let output_block = storage_manager.get_block(output_block_rx.recv().unwrap()).unwrap();
//
//        assert_eq!(output_block.get_row_col(0, 0), input_block.get_row_col(0, 1));
//        assert_eq!(output_block.get_row_col(1, 0), input_block.get_row_col(1, 1));
//        assert_eq!(output_block.get_row_col(2, 0), None);
//        assert_eq!(output_block.get_row_col(0, 1), None);
//
//        storage_manager.clear();
//    }
//}




use hustle_catalog::Table;

use crate::operator::{Operator, WorkOrder};
use std::sync::mpsc::Sender;

pub struct TableReference {
    block_ids: Vec<u64>,
    block_tx: Sender<u64>,
    work_order_tx: Sender<Box<dyn WorkOrder>>,
}

impl TableReference {
    pub fn new(
        table: Table,
        block_tx: Sender<u64>,
        work_order_tx: Sender<Box<dyn WorkOrder>>,
    ) -> Self {
        TableReference {
            block_ids: table.block_ids.clone(),
            block_tx,
            work_order_tx,
        }
    }
}

impl Operator for TableReference {
    fn push_work_orders(&self) {
        println!("TableReference push_work_orders entered");
        for &block_id in self.block_ids.iter() {
            let work_order = Box::new(TableReferenceWorkOrder {
                block_id,
                block_tx: self.block_tx.clone(),
            });
            //work_orders.lock().unwrap().push_back(work_order);
            self.work_order_tx.send(work_order).unwrap();
            println!("TableReferenceWorkOrder sent, block_id: {}", block_id);
        }
        println!("TableReference push_work_orders left");
    }
}

struct TableReferenceWorkOrder {
    block_id: u64,
    block_tx: Sender<u64>,
}

impl WorkOrder for TableReferenceWorkOrder {
    fn execute(&self) {
        self.block_tx.send(self.block_id).unwrap();
        println!("TableReferenceWorkOrder finished, block_id: {}", self.block_id);
    }
}

//#[cfg(test)]
//mod table_reference_tests {
//    use std::sync::mpsc;
//
//    use super::*;
//
//    #[test]
//    fn table_reference() {
//        let storage_manager = StorageManager::with_unique_data_directory();
//        let catalog = Catalog::new();
//        let block_ids = vec![0, 1, 2];
//        let table = Table::with_block_ids("table_reference".to_owned(), vec![], block_ids.clone());
//        let (block_tx, block_rx) = mpsc::channel();
//
//        let table_reference = Box::new(TableReference::new(table.clone(), block_tx));
//        table_reference.execute(&storage_manager, &catalog);
//
//        assert_eq!(block_rx.iter().collect::<Vec<u64>>(), block_ids);
//
//        storage_manager.clear();
//    }
//}

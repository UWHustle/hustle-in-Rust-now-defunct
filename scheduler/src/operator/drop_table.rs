use hustle_catalog::{Catalog, Table};
use hustle_storage::StorageManager;

use crate::operator::{Operator, WorkOrder};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::Sender;

pub struct DropTable {
    storage_manager: Arc<StorageManager>,
    catalog: Arc<Mutex<Catalog>>,
    table: Table,
    work_order_tx: Sender<Box<dyn WorkOrder>>,
}

impl DropTable {
    pub fn new(
        storage_manager: Arc<StorageManager>,
        catalog: Arc<Mutex<Catalog>>,
        table: Table,
        work_order_tx: Sender<Box<dyn WorkOrder>>,
    ) -> Self {
        DropTable {
            storage_manager,
            catalog,
            table,
            work_order_tx
        }
    }
}

impl Operator for DropTable {
    fn push_work_orders(&self) {
        let work_order = Box::new(DropTableWorkOrder {
            storage_manager: self.storage_manager.clone(),
            catalog: self.catalog.clone(),
            table: self.table.clone()
        });
        //work_orders.lock().unwrap().push_back(work_order);
        self.work_order_tx.send(work_order).unwrap();
    }
}

struct DropTableWorkOrder {
    storage_manager: Arc<StorageManager>,
    catalog: Arc<Mutex<Catalog>>,
    table: Table
}

unsafe impl Send for DropTableWorkOrder {}

impl WorkOrder for DropTableWorkOrder {
    fn execute(&self) {
        for &block_id in &self.table.block_ids {
            self.storage_manager.delete_block(block_id);
        }
        self.catalog.lock().unwrap().drop_table(&self.table.name).unwrap();
    }
}


//#[cfg(test)]
//mod drop_table_tests {
//    use super::*;
//
//    #[test]
//    fn drop_table() {
//        let storage_manager = StorageManager::with_unique_data_directory();
//        let catalog = Catalog::new();
//        let table = Table::new("drop_table".to_owned(), vec![]);
//        catalog.create_table(table.clone()).unwrap();
//
//        let drop_table = Box::new(DropTable::new(table));
//        drop_table.execute(&storage_manager, &catalog);
//
//        assert!(!catalog.table_exists("drop_table"));
//
//        storage_manager.clear();
//    }
//}

use hustle_catalog::{Catalog, Table};

use crate::operator::{Operator, WorkOrder};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::Sender;

pub struct CreateTable {
    catalog: Arc<Mutex<Catalog>>,
    table: Table,
    work_order_tx: Sender<Box<dyn WorkOrder>>,
}

impl CreateTable {
    pub fn new(
        catalog: Arc<Mutex<Catalog>>,
        table: Table,
        work_order_tx: Sender<Box<dyn WorkOrder>>,
    ) -> Self {
        CreateTable {
            catalog,
            table,
            work_order_tx
        }
    }
}

impl Operator for CreateTable {
    fn push_work_orders(&self) {
        let work_order = Box::new(CreateTableWorkOrder {
            catalog: self.catalog.clone(), table: self.table.clone()
        });
        self.work_order_tx.send(work_order).unwrap();
    }
}

struct CreateTableWorkOrder {
    catalog: Arc<Mutex<Catalog>>,
    table: Table,
}

impl WorkOrder for CreateTableWorkOrder {
    fn execute(&self) {
        self.catalog.lock().unwrap().create_table(self.table.clone()).unwrap();
    }
}


//#[cfg(test)]
//mod create_table_tests {
//    use super::*;
//
//    #[test]
//    fn create_table() {
//        let storage_manager = StorageManager::with_unique_data_directory();
//        let catalog = Catalog::new();
//        let table = Table::new("create_table".to_owned(), vec![]);
//
//        let create_table = Box::new(CreateTable::new(table));
//        create_table.execute(&storage_manager, &catalog);
//
//        assert!(catalog.table_exists("create_table"));
//
//        storage_manager.clear()
//    }
//}

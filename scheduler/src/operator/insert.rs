use hustle_catalog::Catalog;
use hustle_storage::StorageManager;

use crate::operator::{Operator, WorkOrder};
use crate::router::BlockPoolDestinationRouter;

use std::sync::{Arc, Mutex};
use std::sync::mpsc::Sender;


pub struct Insert {
    storage_manager: Arc<StorageManager>,
    catalog: Arc<Mutex<Catalog>>,
    table_name: String,
    bufs: Arc<Vec<Vec<u8>>>,
    router: Arc<BlockPoolDestinationRouter>,
    work_order_tx: Sender<Box<dyn WorkOrder>>,
}

impl Insert {
    pub fn new(
        storage_manager: Arc<StorageManager>,
        catalog: Arc<Mutex<Catalog>>,
        table_name: String,
        bufs: Vec<Vec<u8>>,
        router: BlockPoolDestinationRouter,
        work_order_tx: Sender<Box<dyn WorkOrder>>,
    ) -> Self {
        Insert {
            storage_manager,
            catalog,
            table_name,
            bufs: Arc::new(bufs),
            router: Arc::new(router),
            work_order_tx
        }
    }
}

unsafe impl Send for Insert {}

impl Operator for Insert {
    fn push_work_orders(&self) {
        let work_order = Box::new(InsertWorkOrder {
            storage_manager: self.storage_manager.clone(),
            catalog: self.catalog.clone(),
            table_name: self.table_name.clone(),
            bufs: self.bufs.clone(),
            router: self.router.clone(),
        });
        //work_orders.lock().unwrap().push_back(work_order);
        self.work_order_tx.send(work_order).unwrap();
    }
}

unsafe impl Send for InsertWorkOrder {}

struct InsertWorkOrder {
    storage_manager: Arc<StorageManager>,
    catalog: Arc<Mutex<Catalog>>,
    table_name: String,
    bufs: Arc<Vec<Vec<u8>>>,
    router: Arc<BlockPoolDestinationRouter>,
}

impl WorkOrder for InsertWorkOrder {
    fn execute(&self) {
        let output_block = self.router.get_block(&self.storage_manager);
        output_block.insert_row(self.bufs.iter().map(|buf| buf.as_slice()));

        for block_id in self.router.get_created_block_ids() {
            self.catalog.lock().unwrap().append_block_id(&self.table_name, block_id).unwrap();
        }
    }
}




//#[cfg(test)]
//mod insert_tests {
//    use hustle_execution_test_util as test_util;
//    use hustle_types::{Bool, Char, HustleType, Int64};
//
//    use crate::operator::Insert;
//
//    use super::*;
//
//    #[test]
//    fn insert() {
//        let storage_manager = StorageManager::with_unique_data_directory();
//        let catalog = Catalog::new();
//        let table = test_util::example_table();
//        catalog.create_table(table.clone()).unwrap();
//        let router = BlockPoolDestinationRouter::new(table.columns);
//
//        let bool_type = Bool;
//        let int64_type = Int64;
//        let char_type = Char::new(1);
//
//        let mut bufs = vec![
//            vec![0; bool_type.byte_len()],
//            vec![0; int64_type.byte_len()],
//            vec![0; char_type.byte_len()],
//        ];
//
//        bool_type.set(false, &mut bufs[0]);
//        int64_type.set(1, &mut bufs[1]);
//        char_type.set("a", &mut bufs[2]);
//
//        let insert = Box::new(Insert::new("insert".to_owned(), bufs.clone(), router));
//        insert.execute(&storage_manager, &catalog);
//
//        storage_manager.clear();
//    }
//}

use std::sync::mpsc::Receiver;
use std::sync::{Mutex, Arc};

use hustle_catalog::{Column, Table};


pub struct Collect {
    columns: Vec<Column>,
    block_ids: Arc<Mutex<Vec<u64>>>,
    block_rx: Receiver<u64>,
}

impl Collect {
    pub fn new(
        columns: Vec<Column>,
        block_rx: Receiver<u64>,
    ) -> Self {
        Collect {
            columns,
            block_ids: Arc::new(Mutex::new(Vec::new())),
            block_rx,
        }
    }

    pub fn get_result(&self) -> Table {
        println!("entering collect.get_result");
        *self.block_ids.lock().unwrap() = self.block_rx.iter().collect();
        Table::with_block_ids(
            String::new(),
            self.columns.clone(),
            self.block_ids.lock().unwrap().clone()
        )
    }
}



//pub struct CollectResult {
//    columns: Vec<Column>,
//    block_ids: Arc<Mutex<Vec<u64>>>,
//}
//
//impl CollectResult {
//    pub fn into_table(self) -> Table {
//        Table::with_block_ids(
//            String::new(),
//            self.columns,
//            self.block_ids.lock().unwrap().clone(),
//        )
//    }
//}

use crate::operator::Operator;
use std::sync::mpsc::Receiver;
use hustle_storage::StorageManager;
use std::sync::Mutex;
use hustle_catalog::{Catalog, Column, Table};

pub struct Collect {
    block_rx: Receiver<u64>,
    operators: Vec<Box<dyn Operator>>,
    cols: Vec<Column>,
    block_ids: Mutex<Vec<u64>>,
}

impl Collect {
    pub fn new(
        block_rx: Receiver<u64>,
        operators: Vec<Box<dyn Operator>>,
        cols: Vec<Column>,
    ) -> Self {
        Collect {
            block_rx,
            operators,
            cols,
            block_ids: Mutex::new(Vec::new()),
        }
    }

    pub fn get_table(&self) -> Table {
        Table::new(String::new(), self.cols.clone(), self.block_ids.lock().unwrap().clone())
    }
}

impl Operator for Collect {
    fn execute(&self, storage_manager: &StorageManager, catalog: &Catalog) {
        for operator in &self.operators {
            operator.execute(storage_manager, catalog);
        }

        *self.block_ids.lock().unwrap() = self.block_rx.iter().collect();
    }
}

use std::sync::{Arc, Mutex};
use std::sync::mpsc::Receiver;

use hustle_catalog::{Catalog, Column, Table};
use hustle_storage::{LogManager, StorageManager};

use crate::operator::Operator;

pub struct Collect {
    operators: Vec<Box<dyn Operator>>,
    columns: Vec<Column>,
    block_ids: Arc<Mutex<Vec<u64>>>,
    block_rx: Receiver<u64>,
}

impl Collect {
    pub fn new(
        operators: Vec<Box<dyn Operator>>,
        columns: Vec<Column>,
        block_rx: Receiver<u64>,
    ) -> Self {
        Collect {
            operators,
            columns,
            block_ids: Arc::new(Mutex::new(Vec::new())),
            block_rx,
        }
    }

    pub fn get_result(&self) -> CollectResult {
        CollectResult {
            columns: self.columns.clone(),
            block_ids: self.block_ids.clone(),
        }
    }
}

impl Operator for Collect {
    fn execute(
        self: Box<Self>,
        storage_manager: &StorageManager,
        log_manager: &LogManager,
        catalog: &Catalog,
    ) {
        for operator in self.operators {
            operator.execute(storage_manager, log_manager, catalog);
        }

        *self.block_ids.lock().unwrap() = self.block_rx.iter().collect();
    }
}

pub struct CollectResult {
    columns: Vec<Column>,
    block_ids: Arc<Mutex<Vec<u64>>>,
}

impl CollectResult {
    pub fn into_table(self) -> Table {
        Table::with_block_ids(
            String::new(),
            self.columns,
            self.block_ids.lock().unwrap().clone(),
        )
    }
}

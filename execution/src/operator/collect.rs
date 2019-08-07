use crate::operator::Operator;
use std::sync::mpsc::Receiver;
use hustle_storage::StorageManager;
use std::sync::Mutex;
use hustle_catalog::Catalog;

pub struct Collect {
    block_rx: Receiver<u64>,
    operators: Vec<Box<dyn Operator>>,
    block_ids: Mutex<Vec<u64>>,
    completed: Mutex<bool>,
}

impl Collect {
    pub fn new(block_rx: Receiver<u64>, operators: Vec<Box<dyn Operator>>) -> Self {
        Collect {
            block_rx,
            operators,
            block_ids: Mutex::new(Vec::new()),
            completed: Mutex::new(false),
        }
    }

    pub fn get_block_ids(&self) -> Vec<u64> {
        self.block_ids.lock().unwrap().clone()
    }
}

impl Operator for Collect {
    fn execute(&self, storage_manager: &StorageManager, catalog: &Catalog) {
        let mut completed = self.completed.lock().unwrap();
        if !*completed {
            for operator in &self.operators {
                operator.execute(storage_manager, catalog);
            }
        }

        *self.block_ids.lock().unwrap() = self.block_rx.iter().collect();
        *completed = true;
    }
}

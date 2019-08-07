use std::sync::mpsc::Sender;
use hustle_catalog::{Table, Catalog};
use crate::operator::Operator;
use hustle_storage::StorageManager;

pub struct TableReference {
    table: Table,
    block_tx: Sender<u64>,
}

impl TableReference {
    pub fn new(
        table: Table,
        block_tx: Sender<u64>,
    ) -> Self {
        TableReference {
            table,
            block_tx,
        }
    }
}

impl Operator for TableReference {
    fn execute(&self, _storage_manager: &StorageManager, catalog: &Catalog) {
        for &block_id in &self.table.block_ids {
            self.block_tx.send(block_id);
        }
    }
}

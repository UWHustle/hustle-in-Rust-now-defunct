use std::sync::mpsc::Sender;

use hustle_catalog::{Catalog, Table};
use hustle_storage::{LogManager, StorageManager};

use crate::operator::Operator;

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
    fn execute(
        self: Box<Self>,
        _storage_manager: &StorageManager,
        _log_manager: &LogManager,
        _catalog: &Catalog
    ) {
        for &block_id in &self.table.block_ids {
            self.block_tx.send(block_id).unwrap();
        }
    }
}

#[cfg(test)]
mod table_reference_tests {
    use std::sync::mpsc;

    use super::*;

    #[test]
    fn table_reference() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let log_manager = LogManager::with_unique_log_directory();
        let catalog = Catalog::new();
        let block_ids = vec![0, 1, 2];
        let table = Table::with_block_ids("table_reference".to_owned(), vec![], block_ids.clone());
        let (block_tx, block_rx) = mpsc::channel();

        let table_reference = Box::new(TableReference::new(table.clone(), block_tx));
        table_reference.execute(&storage_manager, &log_manager, &catalog);

        assert_eq!(block_rx.iter().collect::<Vec<u64>>(), block_ids);

        storage_manager.clear();
    }
}

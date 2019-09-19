use hustle_catalog::{Catalog, Table};
use hustle_storage::{LogManager, StorageManager};

use crate::operator::Operator;

pub struct CreateTable {
    table: Table,
}

impl CreateTable {
    pub fn new(table: Table) -> Self {
        CreateTable { table }
    }
}

impl Operator for CreateTable {
    fn execute(
        self: Box<Self>,
        _storage_manager: &StorageManager,
        _log_manager: &LogManager,
        catalog: &Catalog
    ) {
        catalog.create_table(self.table.clone()).unwrap();
    }
}

#[cfg(test)]
mod create_table_tests {
    use super::*;

    #[test]
    fn create_table() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let log_manager = LogManager::with_unique_log_directory();
        let catalog = Catalog::new();
        let table = Table::new("create_table".to_owned(), vec![]);

        let create_table = Box::new(CreateTable::new(table));
        create_table.execute(&storage_manager, &log_manager, &catalog);

        assert!(catalog.table_exists("create_table"));

        storage_manager.clear();
        log_manager.clear();
    }
}

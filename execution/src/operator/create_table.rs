use hustle_catalog::{Catalog, Table};
use hustle_storage::StorageManager;

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
    fn execute(&self, _storage_manager: &StorageManager, catalog: &Catalog) {
        catalog.create_table(self.table.clone()).unwrap();
    }
}

#[cfg(test)]
mod create_table_tests {
    use super::*;

    #[test]
    fn create_table() {
        let storage_manager = StorageManager::new();
        let catalog = Catalog::new();
        let table = Table::new("create_table".to_owned(), vec![], vec![]);
        CreateTable::new(table).execute(&storage_manager, &catalog);
        assert!(catalog.table_exists("create_table"));
    }
}

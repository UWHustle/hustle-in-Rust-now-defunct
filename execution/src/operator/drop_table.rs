use hustle_catalog::{Catalog, Table};
use hustle_storage::{LogManager, StorageManager};

use crate::operator::Operator;

pub struct DropTable {
    table: Table,
}

impl DropTable {
    pub fn new(table: Table) -> Self {
        DropTable { table }
    }
}

impl Operator for DropTable {
    fn execute(
        self: Box<Self>,
        storage_manager: &StorageManager,
        _log_manager: &LogManager,
        catalog: &Catalog
    ) {
        for &block_id in &self.table.block_ids {
            storage_manager.delete_block(block_id);
        }

        catalog.drop_table(&self.table.name).unwrap();
    }
}

#[cfg(test)]
mod drop_table_tests {
    use super::*;

    #[test]
    fn drop_table() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let log_manager = LogManager::with_unique_log_directory();
        let catalog = Catalog::new();
        let table = Table::new("drop_table".to_owned(), vec![]);
        catalog.create_table(table.clone()).unwrap();

        let drop_table = Box::new(DropTable::new(table));
        drop_table.execute(&storage_manager, &log_manager, &catalog);

        assert!(!catalog.table_exists("drop_table"));

        storage_manager.clear();
        log_manager.clear();
    }
}

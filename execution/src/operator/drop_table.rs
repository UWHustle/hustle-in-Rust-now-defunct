use hustle_catalog::{Catalog, Table};
use hustle_storage::StorageManager;

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
    fn execute(self: Box<Self>, storage_manager: &StorageManager, catalog: &Catalog) {
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
        let storage_manager = StorageManager::default();
        let catalog = Catalog::new();
        let table = Table::new("drop_table".to_owned(), vec![], vec![]);
        catalog.create_table(table.clone()).unwrap();

        let drop_table = DropTable::new(table);
        drop_table.execute(&storage_manager, &catalog);

        assert!(!catalog.table_exists("drop_table"));

        storage_manager.clear();
    }
}

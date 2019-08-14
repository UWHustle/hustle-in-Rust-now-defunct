use hustle_catalog::{Table, Catalog};
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
    fn execute(&self, storage_manager: &StorageManager, catalog: &Catalog) {
        for &block_id in &self.table.block_ids {
            storage_manager.delete_block(block_id);
        }

        catalog.drop_table(&self.table.name).unwrap();
    }
}

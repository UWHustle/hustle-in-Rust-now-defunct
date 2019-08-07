use hustle_catalog::Table;
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
    fn execute(&self, storage_manager: &StorageManager) {
        for &block_id in &self.table.block_ids {
            storage_manager.erase_block(block_id);
        }
    }
}

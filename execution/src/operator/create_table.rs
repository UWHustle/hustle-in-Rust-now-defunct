use crate::operator::Operator;
use hustle_storage::StorageManager;
use hustle_catalog::Table;

pub struct CreateTable {
    table: Table,
}

impl CreateTable {
    pub fn new(table: Table) -> Self {
        CreateTable { table }
    }
}

impl Operator for CreateTable {
    fn execute(&self, _storage_manager: &StorageManager) { }
}

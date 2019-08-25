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

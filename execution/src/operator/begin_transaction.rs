use hustle_catalog::Catalog;
use hustle_storage::StorageManager;

use crate::operator::Operator;

pub struct BeginTransaction;

impl Operator for BeginTransaction {
    fn execute(self: Box<Self>, _storage_manager: &StorageManager, _catalog: &Catalog) {
        // TODO: Implement begin transaction.
    }
}

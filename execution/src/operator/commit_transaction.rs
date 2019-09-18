use hustle_catalog::Catalog;
use hustle_storage::StorageManager;

use crate::operator::Operator;

pub struct CommitTransaction;

impl Operator for CommitTransaction {
    fn execute(self: Box<Self>, _storage_manager: &StorageManager, _catalog: &Catalog) {
        // TODO: Implement commit transaction.
    }
}

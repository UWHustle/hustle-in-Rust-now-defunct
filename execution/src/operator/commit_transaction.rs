use hustle_catalog::Catalog;
use hustle_storage::{LogManager, StorageManager};

use crate::operator::Operator;

pub struct CommitTransaction {
    transaction_id: u64,
}

impl CommitTransaction {
    pub fn new(transaction_id: u64) -> Self {
        CommitTransaction {
            transaction_id,
        }
    }
}

impl Operator for CommitTransaction {
    fn execute(
        self: Box<Self>,
        _storage_manager: &StorageManager,
        log_manager: &LogManager,
        _catalog: &Catalog,
    ) {
        log_manager.log_commit_transaction(self.transaction_id);
    }
}

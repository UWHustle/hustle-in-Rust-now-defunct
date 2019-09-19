use hustle_catalog::Catalog;
use hustle_storage::{LogManager, StorageManager};

use crate::operator::Operator;

pub struct BeginTransaction {
    transaction_id: u64,
}

impl BeginTransaction {
    pub fn new(transaction_id: u64) -> Self {
        BeginTransaction {
            transaction_id,
        }
    }
}

impl Operator for BeginTransaction {
    fn execute(
        self: Box<Self>,
        _storage_manager: &StorageManager,
        log_manager: &LogManager,
        _catalog: &Catalog
    ) {
        log_manager.log_begin_transaction(self.transaction_id);
    }
}

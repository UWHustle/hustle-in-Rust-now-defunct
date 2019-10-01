use hustle_catalog::Catalog;
use hustle_storage::{LogManager, StorageManager};

use crate::operator::Operator;
use crate::state::TransactionState;
use std::sync::Arc;

pub struct BeginTransaction {
    transaction_state: Arc<TransactionState>,
}

impl BeginTransaction {
    pub fn new(transaction_state: Arc<TransactionState>) -> Self {
        BeginTransaction {
            transaction_state,
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
        log_manager.log_begin_transaction(self.transaction_state.id);
    }
}

use std::sync::Arc;

use hustle_catalog::Catalog;
use hustle_storage::{LogManager, StorageManager};

use crate::operator::Operator;
use crate::state::TransactionState;

pub struct CommitTransaction {
    transaction_state: Arc<TransactionState>,
}

impl CommitTransaction {
    pub fn new(transaction_state: Arc<TransactionState>) -> Self {
        CommitTransaction {
            transaction_state,
        }
    }
}

impl Operator for CommitTransaction {
    fn execute(
        self: Box<Self>,
        storage_manager: &StorageManager,
        log_manager: &LogManager,
        _catalog: &Catalog,
    ) {
        log_manager.log_commit_transaction(self.transaction_state.id);

        for (&block_id, row_ids) in &*self.transaction_state.get_tentative().lock().unwrap() {
            let block = storage_manager.get_block(block_id).unwrap();
            for &row_id in &*row_ids.lock().unwrap() {
                block.finalize_row(row_id as usize);
            }
        }

        log_manager.log_complete_transaction(self.transaction_state.id);
    }
}

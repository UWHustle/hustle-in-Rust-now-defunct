use std::collections::HashMap;

use hustle_catalog::Catalog;
use hustle_storage::{LogManager, StorageManager};

use crate::engine::FinalizeRowIds;
use crate::operator::Operator;

pub struct CommitTransaction {
    transaction_id: u64,
    finalize_row_ids: FinalizeRowIds,
}

impl CommitTransaction {
    pub fn new(transaction_id: u64, finalize_row_ids: FinalizeRowIds) -> Self {
        CommitTransaction {
            transaction_id,
            finalize_row_ids,
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
        log_manager.log_commit_transaction(self.transaction_id);

        if let Some(finalize_row_ids) = self.finalize_row_ids
            .lock()
            .unwrap()
            .remove(&self.transaction_id)
        {
            for (block_id, row_ids) in finalize_row_ids {
                let block = storage_manager.get_block(block_id).unwrap();
                for row_id in row_ids {
                    block.finalize_row(row_id as usize);
                }
            }
        }

        log_manager.log_complete_transaction(self.transaction_id);
    }
}

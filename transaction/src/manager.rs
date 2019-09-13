use std::collections::HashMap;

use hustle_common::plan::Plan;

use crate::policy::{Policy, ZeroConcurrencyPolicy};

pub struct TransactionManager {
    policy: Box<dyn Policy + Send>,
    transaction_ids: HashMap<u64, u64>,
}

impl TransactionManager {
    pub fn new() -> Self {
        TransactionManager {
            policy: Box::new(ZeroConcurrencyPolicy::new()),
            transaction_ids: HashMap::new(),
        }
    }

    pub fn begin_transaction(&mut self, connection_id: u64) -> Result<(), String> {
        if self.transaction_ids.contains_key(&connection_id) {
            Err("Cannot begin a transaction within a transaction".to_owned())
        } else {
            let transaction_id = self.policy.begin_transaction();
            self.transaction_ids.insert(connection_id, transaction_id);
            Ok(())
        }
    }

    pub fn commit_transaction(&mut self, connection_id: u64) -> Result<Vec<(Plan, u64)>, String> {
        if let Some(transaction_id) = self.transaction_ids.remove(&connection_id) {
            Ok(self.policy.commit_transaction(transaction_id))
        } else {
            Err("Cannot commit when no transaction is active".to_owned())
        }
    }

    pub fn enqueue_statement(&mut self, plan: Plan, connection_id: u64) -> Vec<(Plan, u64)> {
        if let Some(transaction_id) = self.transaction_ids.get(&connection_id) {
            self.policy.enqueue_statement(transaction_id.to_owned(), plan)
        } else {
            let transaction_id = self.policy.begin_transaction();
            let mut statements = self.policy.enqueue_statement(transaction_id, plan);
            statements.append(&mut self.policy.commit_transaction(transaction_id));
            statements
        }
    }

    pub fn complete_statement(&mut self, statement_id: u64) -> Vec<(Plan, u64)> {
        self.policy.complete_statement(statement_id)
    }

    pub fn close_connection(&mut self, connection_id: u64) -> Vec<(Plan, u64)> {
        // TODO: Rollback transaction on connection close.
        // We don't support transaction rollback so when a connection closes we just commit.
        if let Some(transaction_id) = self.transaction_ids.remove(&connection_id) {
            self.policy.commit_transaction(transaction_id.to_owned())
        } else {
            vec![]
        }
    }
}

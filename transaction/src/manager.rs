use std::collections::HashMap;

use hustle_common::plan::{Plan, Statement};

use crate::policy::{Policy, ZeroConcurrencyPolicy};

pub struct TransactionManager {
    policy: Box<dyn Policy + Send>,
    transaction_ids: HashMap<u64, u64>,
    statement_ctr: u64,
    transaction_ctr: u64,
}

impl TransactionManager {
    pub fn new() -> Self {
        TransactionManager {
            policy: Box::new(ZeroConcurrencyPolicy::new()),
            transaction_ids: HashMap::new(),
            statement_ctr: 0,
            transaction_ctr: 0,
        }
    }

    pub fn transact_plan(
        &mut self,
        plan: Plan,
        connection_id: u64,
    ) -> Result<Vec<Statement>, String> {
        match plan {
            Plan::BeginTransaction => {
                if self.transaction_ids.contains_key(&connection_id) {
                    Err("Cannot begin a transaction within a transaction".to_owned())
                } else {
                    let statement_id = self.new_statement_id();
                    let transaction_id = self.new_transaction_id();
                    let statement = Statement { id: statement_id, transaction_id, plan };
                    self.transaction_ids.insert(connection_id, transaction_id);
                    Ok(self.policy.enqueue_statement(statement))
                }
            }
            _ => {
                if let Some(&transaction_id) = self.transaction_ids.get(&connection_id) {
                    let statement = Statement {
                        id: self.new_statement_id(),
                        transaction_id,
                        plan
                    };

                    Ok(self.policy.enqueue_statement(statement))
                } else {
                    let transaction_id = self.new_transaction_id();

                    let begin = Statement {
                        id: self.new_statement_id(),
                        transaction_id,
                        plan: Plan::BeginTransaction
                    };

                    let statement = Statement {
                        id: self.new_statement_id(),
                        transaction_id,
                        plan,
                    };

                    let commit = Statement {
                        id: self.new_statement_id(),
                        transaction_id,
                        plan: Plan::CommitTransaction,
                    };

                    Ok(self.policy.enqueue_statement(begin).into_iter()
                        .chain(self.policy.enqueue_statement(statement))
                        .chain(self.policy.enqueue_statement(commit))
                        .collect())
                }
            }
        }
    }

    pub fn complete_statement(
        &mut self,
        statement: Statement,
        connection_id: u64,
    ) -> Result<Vec<Statement>, String> {
        match statement.plan {
            Plan::CommitTransaction => {
                self.transaction_ids.remove(&connection_id)
                    .map(|_| self.policy.complete_statement(statement))
                    .ok_or("Cannot commit when no transaction is active".to_owned())
            },
            _ => Ok(self.policy.complete_statement(statement))
        }
    }

    pub fn close_connection(&mut self, connection_id: u64) -> Vec<Statement> {
        // TODO: Rollback transaction on connection close.
        // We don't support transaction rollback so when a connection closes we just commit.
        if let Some(transaction_id) = self.transaction_ids.remove(&connection_id) {
            let commit = Statement {
                id: self.new_statement_id(),
                transaction_id,
                plan: Plan::CommitTransaction,
            };
            self.policy.enqueue_statement(commit)
        } else {
            vec![]
        }
    }

    fn new_statement_id(&mut self) -> u64 {
        Self::new_id(&mut self.statement_ctr)
    }

    fn new_transaction_id(&mut self) -> u64 {
        Self::new_id(&mut self.transaction_ctr)
    }

    fn new_id(ctr: &mut u64) -> u64 {
        let id = *ctr;
        *ctr = ctr.wrapping_add(1);
        id
    }
}

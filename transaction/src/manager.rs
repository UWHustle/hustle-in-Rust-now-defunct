use std::collections::HashMap;

use hustle_common::plan::{Plan, Statement};

use crate::policy::{Policy, PredicateComparisonPolicy};

pub struct TransactionManager {
    policy: Box<dyn Policy + Send>,
    transaction_ids: HashMap<u64, u64>,
    statement_ctr: u64,
    transaction_ctr: u64,
}

impl TransactionManager {
    pub fn new() -> Self {
        TransactionManager {
            policy: Box::new(PredicateComparisonPolicy::new()),
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
                    let transaction_id = self.new_transaction_id();

                    let statement = Statement::new(
                        self.new_statement_id(),
                        transaction_id,
                        connection_id,
                        plan,
                    );

                    self.transaction_ids.insert(connection_id, transaction_id);
                    Ok(self.policy.enqueue_statement(statement))
                }
            },
            Plan::CommitTransaction => {
                if let Some(transaction_id) = self.transaction_ids.remove(&connection_id) {
                    let statement = Statement::new(
                        self.new_statement_id(),
                        transaction_id,
                        connection_id,
                        plan,
                    );

                    Ok(self.policy.enqueue_statement(statement))
                } else {
                    Err("Cannot commit when no transaction is active".to_owned())
                }
            },
            _ => {
                if let Some(&transaction_id) = self.transaction_ids.get(&connection_id) {
                    let statement = Statement::new(
                        self.new_statement_id(),
                        transaction_id,
                        connection_id,
                        plan,
                    );

                    Ok(self.policy.enqueue_statement(statement))
                } else {
                    let transaction_id = self.new_transaction_id();

                    let begin = Statement::silent(
                        self.new_statement_id(),
                        transaction_id,
                        connection_id,
                        Plan::BeginTransaction,
                    );

                    let statement = Statement::new(
                        self.new_statement_id(),
                        transaction_id,
                        connection_id,
                        plan,
                    );

                    let commit = Statement::silent(
                        self.new_statement_id(),
                        transaction_id,
                        connection_id,
                        Plan::CommitTransaction,
                    );

                    Ok(self.policy.enqueue_statement(begin).into_iter()
                            .chain(self.policy.enqueue_statement(statement))
                            .chain(self.policy.enqueue_statement(commit))
                            .collect())
                }
            }
        }
    }

    pub fn complete_statement(&mut self, statement: Statement) -> Result<Vec<Statement>, String> {
        Ok(self.policy.complete_statement(statement))
    }

    pub fn close_connection(&mut self, connection_id: u64) -> Vec<Statement> {
        // TODO: Rollback transaction on connection close.
        // We don't support transaction rollback so when a connection closes we just commit.
        if let Some(transaction_id) = self.transaction_ids.remove(&connection_id) {
            let commit = Statement::new(
                self.new_statement_id(),
                transaction_id,
                connection_id,
                Plan::CommitTransaction,
            );
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

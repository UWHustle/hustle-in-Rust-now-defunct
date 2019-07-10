use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};

use message::{Message, Plan};

use crate::policy::{Policy, ZeroConcurrencyPolicy};

pub struct TransactionManager {
    policy: Box<Policy + Send>,
    transaction_ids: HashMap<u64, u64>,
}

impl TransactionManager {
    pub fn new() -> Self {
        TransactionManager {
            policy: Box::new(ZeroConcurrencyPolicy::new()),
            transaction_ids: HashMap::new(),
        }
    }

    pub fn listen(
        &mut self,
        transaction_rx: Receiver<Vec<u8>>,
        execution_tx: Sender<Vec<u8>>,
        completed_tx: Sender<Vec<u8>>,
    ) {
        loop {
            let buf = transaction_rx.recv().unwrap();
            let request = Message::deserialize(&buf).unwrap();

            // Process the message contents.
            match request {
                Message::TransactPlan { plan, connection_id } => {
                    match plan {
                        Plan::BeginTransaction => self.begin_transaction(
                            connection_id,
                            &completed_tx,
                        ),
                        Plan::CommitTransaction => self.commit_transaction(
                            connection_id,
                            &completed_tx,
                            &|admit_plan, statement_id| Self::admit_statement(
                                admit_plan,
                                statement_id,
                                connection_id,
                                &execution_tx,
                            ),
                        ),
                        _ => self.enqueue_statement(
                            plan,
                            connection_id,
                            &|admit_plan, statement_id| Self::admit_statement(
                                admit_plan,
                                statement_id,
                                connection_id,
                                &execution_tx,
                            )
                        ),
                    }
                },
                Message::CompletePlan { statement_id, connection_id } => self.complete_statement(
                    statement_id,
                    &|admit_plan, statement_id| Self::admit_statement(
                        admit_plan,
                        statement_id,
                        connection_id,
                        &execution_tx
                    ),
                ),
                Message::CloseConnection { connection_id } => self.close(
                    connection_id,
                    &|admit_plan, statement_id| Self::admit_statement(
                        admit_plan,
                        statement_id,
                        connection_id,
                        &execution_tx
                    ),
                ),
                _ => completed_tx.send(buf).unwrap()
            };
        }
    }

    fn begin_transaction(&mut self, connection_id: u64, completed_tx: &Sender<Vec<u8>>) {
        let response = if self.transaction_ids.contains_key(&connection_id) {
            Message::Failure {
                reason: "Cannot begin a transaction within a transaction".to_string(),
                connection_id
            }
        } else {
            let transaction_id = self.policy.begin_transaction();
            self.transaction_ids.insert(connection_id, transaction_id);
            Message::Success { connection_id }
        };
        completed_tx.send(response.serialize().unwrap()).unwrap();
    }

    fn commit_transaction(
        &mut self,
        connection_id: u64,
        completed_tx: &Sender<Vec<u8>>,
        admit_statement: &Fn(Plan, u64),
    ) {
        let response = if let Some(transaction_id) = self.transaction_ids.remove(&connection_id) {
            self.policy.commit_transaction(transaction_id, admit_statement);
            Message::Success { connection_id }
        } else {
            Message::Failure {
                reason: "Cannot commit when no transaction is active".to_string(),
                connection_id
            }
        };
        completed_tx.send(response.serialize().unwrap()).unwrap();
    }

    fn enqueue_statement(
        &mut self,
        plan: Plan,
        connection_id: u64,
        admit_statement: &Fn(Plan, u64),
    ) {
        if let Some(transaction_id) = self.transaction_ids.get(&connection_id) {
            self.policy.enqueue_statement(transaction_id.to_owned(), plan, admit_statement);
        } else {
            let transaction_id = self.policy.begin_transaction();
            self.policy.enqueue_statement(transaction_id, plan, admit_statement);
            self.policy.commit_transaction(transaction_id, admit_statement);
        }
    }

    fn complete_statement(
        &mut self,
        statement_id: u64,
        admit_statement: &Fn(Plan, u64),
    ) {
        self.policy.complete_statement(statement_id, admit_statement);
    }

    fn close(&mut self, connection_id: u64, admit_statement: &Fn(Plan, u64)) {
        // TODO: Rollback transaction on connection close.
        // We don't support transaction rollback so when a connection closes we just commit.
        if let Some(transaction_id) = self.transaction_ids.remove(&connection_id) {
            self.policy.commit_transaction(transaction_id.to_owned(), admit_statement);
        }
    }

    fn admit_statement(
        plan: Plan,
        statement_id: u64,
        connection_id: u64,
        execution_tx: &Sender<Vec<u8>>
    ) {
        execution_tx.send(Message::ExecutePlan {
            plan,
            statement_id,
            connection_id,
        }.serialize().unwrap()).unwrap();
    }
}

use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};

use hustle_common::message::Message;
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
                            &execution_tx,
                            &completed_tx,
                        ),
                        _ => self.enqueue_statement(
                            plan,
                            connection_id,
                            &execution_tx
                        ),
                    }
                },
                Message::CompletePlan { statement_id, connection_id } => self.complete_statement(
                    statement_id,
                    connection_id,
                    &execution_tx,
                ),
                Message::CloseConnection { connection_id } => self.close(
                    connection_id,
                    &execution_tx,
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
        execution_tx: &Sender<Vec<u8>>,
        completed_tx: &Sender<Vec<u8>>,
    ) {
        let response = if let Some(transaction_id) = self.transaction_ids.remove(&connection_id) {
            Self::admit_statements(
                self.policy.commit_transaction(transaction_id),
                connection_id,
                execution_tx
            );
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
        execution_tx: &Sender<Vec<u8>>,
    ) {
        if let Some(transaction_id) = self.transaction_ids.get(&connection_id) {
            Self::admit_statements(
                self.policy.enqueue_statement(transaction_id.to_owned(), plan),
                connection_id,
                execution_tx
            );
        } else {
            let transaction_id = self.policy.begin_transaction();
            Self::admit_statements(
                self.policy.enqueue_statement(transaction_id, plan),
                connection_id,
                execution_tx,
            );
            Self::admit_statements(
                self.policy.commit_transaction(transaction_id),
                connection_id,
                execution_tx,
            );
        }
    }

    fn complete_statement(
        &mut self,
        statement_id: u64,
        connection_id: u64,
        execution_tx: &Sender<Vec<u8>>,
    ) {
        Self::admit_statements(
            self.policy.complete_statement(statement_id),
            connection_id,
            execution_tx,
        );
    }

    fn close(&mut self, connection_id: u64, execution_tx: &Sender<Vec<u8>>) {
        // TODO: Rollback transaction on connection close.
        // We don't support transaction rollback so when a connection closes we just commit.
        if let Some(transaction_id) = self.transaction_ids.remove(&connection_id) {
            Self::admit_statements(
                self.policy.commit_transaction(transaction_id.to_owned()),
                connection_id,
                execution_tx,
            );
        }
    }

    fn admit_statements(
        statements: Vec<(Plan, u64)>,
        connection_id: u64,
        execution_tx: &Sender<Vec<u8>>,
    ) {
        for (plan, statement_id) in statements {
            execution_tx.send(Message::ExecutePlan {
                plan,
                statement_id,
                connection_id,
            }.serialize().unwrap()).unwrap();
        }
    }
}

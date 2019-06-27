use std::sync::mpsc::{Receiver, Sender};
use std::collections::HashMap;
use message::{Message, Plan, Statement};
use crate::Transaction;
use crate::policy::{Policy, ZeroConcurrencyPolicy};

pub struct TransactionManager {
    policy: Box<Policy + Send>,
    transaction_ids: HashMap<u64, u64>,
    transaction_ctr: u64,
    statement_ctr: u64,
}

impl TransactionManager {
    pub fn new() -> Self {
        TransactionManager {
            policy: Box::new(ZeroConcurrencyPolicy::new()),
            transaction_ids: HashMap::new(),
            transaction_ctr: 0,
            statement_ctr: 0,
        }
    }

    pub fn listen(
        &mut self,
        transaction_rx: Receiver<Vec<u8>>,
        _execution_tx: Sender<Vec<u8>>,
        completed_tx: Sender<Vec<u8>>,
    ) {
        loop {
            let buf = transaction_rx.recv().unwrap();
            let request = Message::deserialize(&buf).unwrap();

            // Process the message contents.
            match request {
                Message::BeginTransaction { connection_id } =>
                    self.begin_transaction(connection_id, &completed_tx),
                Message::CommitTransaction { connection_id } =>
                    self.commit_transaction(connection_id, &completed_tx),
                Message::TransactPlan { plan, connection_id } =>
                    self.enqueue_statement(plan, connection_id),
                Message::CompleteStatement { statement: _ } =>
                    println!("complete statement"),
                Message::CloseConnection { connection_id } =>
                    self.close(connection_id),
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
            let transaction = Transaction::new(&mut self.transaction_ctr);
            self.transaction_ids.insert(connection_id, transaction.id);
            self.policy.begin_transaction(transaction);
            Message::Success { connection_id }
        };
        completed_tx.send(response.serialize().unwrap()).unwrap();
    }

    fn commit_transaction(&mut self, connection_id: u64, completed_tx: &Sender<Vec<u8>>) {
        let response = if let Some(transaction_id) = self.transaction_ids.remove(&connection_id) {
            self.policy.commit_transaction(transaction_id);
            Message::Success { connection_id }
        } else {
            Message::Failure {
                reason: "Cannot commit when no transaction is active".to_string(),
                connection_id
            }
        };
        completed_tx.send(response.serialize().unwrap()).unwrap();
    }

    fn enqueue_statement(&mut self, plan: Plan, connection_id: u64) {
        let statement_id = self.generate_statement_id();
        if let Some(transaction_id) = self.transaction_ids.get(&connection_id) {
            let statement = Statement::new(statement_id, transaction_id.to_owned(), connection_id, plan);
            self.policy.enqueue_statement(transaction_id.to_owned(), statement);
        } else {
            let transaction = Transaction::new(&mut self.transaction_ctr);
            let transaction_id = transaction.id;
            let statement = Statement::new(statement_id, transaction_id, connection_id, plan);
            self.policy.begin_transaction(transaction);
            self.policy.enqueue_statement(transaction_id, statement);
            self.policy.commit_transaction(transaction_id);
        }
    }

    fn close(&mut self, _connection_id: u64) {
        // TODO: Rollback transaction on connection close.
        // We don't support transaction rollback so when a connection closes we just commit.
    }

    fn generate_statement_id(&mut self) -> u64 {
        let id = self.statement_ctr;
        self.statement_ctr.wrapping_add(1);
        id
    }
}

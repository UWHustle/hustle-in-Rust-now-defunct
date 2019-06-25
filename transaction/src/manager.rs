use std::sync::mpsc::{Receiver, Sender};
use std::collections::{VecDeque, HashMap};
use message::{Message, Plan, Statement};
use crate::Transaction;

pub struct TransactionManager {
    transaction_queue: VecDeque<u64>,
    transaction_map: HashMap<u64, Transaction>
}

impl TransactionManager {
    pub fn new() -> Self {
        TransactionManager {
            transaction_queue: VecDeque::new(),
            transaction_map: HashMap::new()
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
                Message::BeginTransaction { connection_id } =>
                    self.begin(connection_id, &execution_tx),
                Message::CommitTransaction { connection_id } =>
                    self.commit(connection_id, &execution_tx),
                Message::TransactPlan { plan, connection_id } =>
                    self.execute(plan, connection_id),
                Message::CloseConnection { connection_id } =>
                    self.close(connection_id),
                _ => completed_tx.send(buf).unwrap()
            };

            // Serialize each transaction.
            while let Some(connection_id) = self.transaction_queue.front() {
                let transaction = self.transaction_map.get_mut(connection_id).unwrap();

                // Send all the statements of the front transaction.
                while let Some(plan) = transaction.dequeue_plan() {
                    execution_tx.send(Message::ExecuteStatement {
                        statement: Statement::new(0, 0, plan),
                        connection_id: connection_id.clone()
                    }.serialize().unwrap()).unwrap();
                }

                // If the front transaction is committed, move on to the next transaction. Else,
                // continue to receive statements.
                if transaction.committed {
                    self.transaction_map.remove(&self.transaction_queue.pop_front().unwrap());
                } else {
                    break;
                }
            }
        }
    }

    fn begin(&mut self, connection_id: u64, completed_tx: &Sender<Vec<u8>>) {
        let response = if self.transaction_map.contains_key(&connection_id) {
            Message::Failure {
                reason: "Cannot begin a transaction within a transaction".to_string(),
                connection_id
            }
        } else {
            self.transaction_queue.push_back(connection_id);
            self.transaction_map.insert(connection_id, Transaction::new());
            Message::Success { connection_id }
        };
        completed_tx.send(response.serialize().unwrap()).unwrap();
    }

    fn commit(&mut self, connection_id: u64, completed_tx: &Sender<Vec<u8>>) {
        let response = if let Some(transaction) = self.transaction_map.get_mut(&connection_id) {
            transaction.committed = true;
            Message::Success { connection_id }
        } else {
            Message::Failure {
                reason: "Cannot commit when no transaction is active".to_string(),
                connection_id
            }
        };
        completed_tx.send(response.serialize().unwrap()).unwrap();
    }

    fn execute(&mut self, plan: Plan, connection_id: u64) {
        if let Some(transaction) = self.transaction_map.get_mut(&connection_id) {
            transaction.enqueue_plan(plan);
        } else {
            let mut transaction = Transaction::new();
            transaction.enqueue_plan(plan);
            transaction.committed = true;
            self.transaction_queue.push_back(connection_id);
            self.transaction_map.insert(connection_id, transaction);
        }
    }

    fn close(&mut self, connection_id: u64) {
        // TODO: Rollback transaction on connection close.
        // We don't support transaction rollback so when a connection closes we just commit.
        if let Some(transaction) = self.transaction_map.get_mut(&connection_id) {
            transaction.committed = true;
        }
    }
}

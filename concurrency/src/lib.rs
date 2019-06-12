use std::sync::mpsc::{Receiver, Sender};
use std::collections::{VecDeque, HashMap};
use message::Message;

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
        input_rx: Receiver<Vec<u8>>,
        execution_tx: Sender<Vec<u8>>,
        completed_tx: Sender<Vec<u8>>,
    ) {
        loop {
            let buf = input_rx.recv().unwrap();
            let request = Message::deserialize(&buf).unwrap();

            // Process the message contents.
            match request {
                Message::BeginTransaction { connection_id } =>
                    self.begin(connection_id, &completed_tx),
                Message::CommitTransaction { connection_id } =>
                    self.commit(connection_id, &completed_tx),
                Message::ExecutePlan { plan, connection_id } =>
                    self.execute(plan, connection_id),
                _ => panic!("Invalid message type sent to transaction manager")
            };

            // Serialize each transaction.
            while let Some(connection_id) = self.transaction_queue.front() {
                let transaction = self.transaction_map.get_mut(connection_id).unwrap();

                // Send all the statements of the front transaction.
                while let Some(plan) = transaction.statements.pop_front() {
                    let response = Message::ExecutePlan {
                        plan,
                        connection_id: connection_id.clone()
                    };
                    execution_tx.send(response.serialize().unwrap()).unwrap();
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
            Message::Error {
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
            Message::Error {
                reason: "Cannot commit when no transaction is active".to_string(),
                connection_id
            }
        };
        completed_tx.send(response.serialize().unwrap()).unwrap();
    }

    fn execute(&mut self, plan: String, connection_id: u64) {
        if let Some(transaction) = self.transaction_map.get_mut(&connection_id) {
            transaction.statements.push_back(plan);
        } else {
            let mut transaction = Transaction::new();
            transaction.statements.push_back(plan);
            transaction.committed = true;
            self.transaction_queue.push_back(connection_id);
            self.transaction_map.insert(connection_id, transaction);
        }
    }
}

pub struct Transaction {
    statements: VecDeque<String>,
    committed: bool
}

impl Transaction {
    fn new() -> Self {
        Transaction {
            statements: VecDeque::new(),
            committed: false
        }
    }
}

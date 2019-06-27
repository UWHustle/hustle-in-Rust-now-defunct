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
        execution_tx: Sender<Vec<u8>>,
        completed_tx: Sender<Vec<u8>>,
    ) {
        loop {
            let buf = transaction_rx.recv().unwrap();
            let request = Message::deserialize(&buf).unwrap();

            // Process the message contents.
            match request {
                Message::TransactPlan { plan, connection_id } => self.transact_plan(
                    plan,
                    connection_id,
                    &execution_tx,
                    &completed_tx,
                ),
                Message::CompleteStatement { statement } => self.complete_statement(
                    statement,
                    &execution_tx,
                ),
                Message::CloseConnection { connection_id } => self.close(connection_id),
                _ => completed_tx.send(buf).unwrap()
            };
        }
    }

    fn transact_plan(
        &mut self,
        plan: Plan,
        connection_id: u64,
        execution_tx: &Sender<Vec<u8>>,
        completed_tx: &Sender<Vec<u8>>,
    ) {
        match plan {
            Plan::BeginTransaction => self.begin_transaction(connection_id, completed_tx),
            Plan::CommitTransaction => self.commit_transaction(
                connection_id,
                execution_tx,
                completed_tx
            ),
            _ => self.enqueue_statement(plan, connection_id, execution_tx),
        }
    }

    fn begin_transaction(&mut self, connection_id: u64, completed_tx: &Sender<Vec<u8>>) {
        let response = if self.transaction_ids.contains_key(&connection_id) {
            Message::Failure {
                reason: "Cannot begin a transaction within a transaction".to_string(),
                connection_id
            }
        } else {
            let transaction = Transaction::new(self.generate_transaction_id());
            self.transaction_ids.insert(connection_id, transaction.id);
            self.policy.begin_transaction(transaction);
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
            let statements = self.policy.commit_transaction(transaction_id);
            self.execute_statements(statements, execution_tx);
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
        let statement_id = self.generate_statement_id();
        if let Some(transaction_id) = self.transaction_ids.get(&connection_id) {
            let statement = Statement::new(statement_id, transaction_id.to_owned(), connection_id, plan);
            let statements = self.policy.enqueue_statement(statement);
            self.execute_statements(statements, execution_tx);
        } else {
            let transaction = Transaction::new(self.generate_transaction_id());
            let transaction_id = transaction.id;
            let statement = Statement::new(statement_id, transaction_id, connection_id, plan);
            self.policy.begin_transaction(transaction);
            let statements = self.policy.enqueue_statement(statement);
            self.execute_statements(statements, execution_tx);
            let statements = self.policy.commit_transaction(transaction_id);
            self.execute_statements(statements, execution_tx);
        }
    }

    fn complete_statement(&mut self, statement: Statement, execution_tx: &Sender<Vec<u8>>) {
        let statements = self.policy.complete_statement(statement);
        self.execute_statements(statements, execution_tx)
    }

    fn close(&mut self, _connection_id: u64) {
        // TODO: Rollback transaction on connection close.
        // We don't support transaction rollback so when a connection closes we just commit.
        for transaction_id in self.transaction_ids.values() {
            self.policy.commit_transaction(transaction_id.to_owned());
        }
    }

    fn execute_statements(&self, statements: Vec<Statement>, execution_tx: &Sender<Vec<u8>>) {
        for statement in statements {
            execution_tx.send(Message::ExecuteStatement {
                statement
            }.serialize().unwrap()).unwrap();
        }
    }

    fn generate_statement_id(&mut self) -> u64 {
        let id = self.statement_ctr;
        self.statement_ctr = self.statement_ctr.wrapping_add(1);
        id
    }

    fn generate_transaction_id(&mut self) -> u64 {
        let id = self.transaction_ctr;
        self.transaction_ctr = self.transaction_ctr.wrapping_add(1);
        id
    }
}

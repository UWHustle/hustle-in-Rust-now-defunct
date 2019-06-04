use std::sync::{Mutex, Arc, Condvar};
use std::collections::VecDeque;
use std::thread;
use execution::logical_entities::relation::Relation;
use execution::ExecutionEngine;
use std::sync::mpsc::{self, Sender};

pub struct TransactionManager {
    transaction_sender: Sender<Arc<Transaction>>
}

impl TransactionManager {
    pub fn new(execution_engine: Arc<ExecutionEngine>) -> Self {

        // Spawn transaction processing thread.
        let (transaction_sender, transaction_receiver) = mpsc::channel::<Arc<Transaction>>();
        thread::spawn(move || {
            loop {
                if let Some(transaction) = transaction_receiver.recv().ok() {
                    // Process the transaction.
                    while let Some(statement) = transaction.pop_front_statement() {
                        let result = execution_engine.execute_plan(&statement.plan);
                        statement.set_result(Ok(result));
                    }
                }
            }
        });

        TransactionManager {
            transaction_sender
        }
    }

    pub fn connect(&self) -> TransactionManagerConnection {
        TransactionManagerConnection::new(Sender::clone(&self.transaction_sender))
    }
}

pub struct TransactionManagerConnection {
    current_transaction: Option<Arc<Transaction>>,
    transaction_sender: Sender<Arc<Transaction>>
}

impl TransactionManagerConnection {
    fn new(transaction_sender: Sender<Arc<Transaction>>) -> Self {
        TransactionManagerConnection {
            current_transaction: None,
            transaction_sender
        }
    }

    pub fn begin_transaction(&mut self) -> Result<(), String> {
        if self.current_transaction.is_none() {
            let transaction = Arc::new(Transaction::new());
            self.current_transaction = Some(transaction.clone());
            self.transaction_sender.send(transaction).map_err(|e| e.to_string())?;
            Ok(())
        } else {
            Err("Cannot begin a transaction within a transaction".to_string())
        }
    }

    pub fn execute_statement(&mut self, plan: String) -> Result<Option<Relation>, String> {
        let statement = Arc::new(TransactionStatement::new(plan));

        if let Some(transaction) = &self.current_transaction {
            transaction.push_back_statement(statement.clone());
        } else {
            self.begin_transaction()?;
            self.current_transaction.as_ref().unwrap().push_back_statement(statement.clone());
            self.commit_transaction()?;
        }

        statement.await_result()
    }

    pub fn commit_transaction(&mut self) -> Result<(), String> {
        if let Some(transaction) = self.current_transaction.take() {
            transaction.commit();
            Ok(())
        } else {
            Err("Cannot commit when no transaction is active".to_string())
        }
    }
}

struct Transaction {
    statements: (Mutex<VecDeque<Option<Arc<TransactionStatement>>>>, Condvar)
}

impl Transaction {
    fn new() -> Self {
        Transaction {
            statements: (Mutex::new(VecDeque::new()), Condvar::new())
        }
    }

    fn push_back_statement(&self, statement: Arc<TransactionStatement>) {
        self.statements.0.lock().unwrap().push_back(Some(statement));
    }

    fn pop_front_statement(&self) -> Option<Arc<TransactionStatement>> {
        let &(ref lock, ref cvar) = &self.statements;
        let mut statements = lock.lock().unwrap();
        loop {
            if let Some(statement) = statements.pop_front() {
                break statement;
            }
            statements = cvar.wait(statements).unwrap();
        }
    }

    fn commit(&self) {
        self.statements.0.lock().unwrap().push_back(None);
    }
}

struct TransactionStatement {
    plan: String,
    result: Arc<(Mutex<Option<Result<Option<Relation>, String>>>, Condvar)>
}

impl TransactionStatement {
    pub fn new(plan: String) -> Self {
        TransactionStatement {
            plan,
            result: Arc::new((Mutex::new(None), Condvar::new()))
        }
    }

    pub fn set_result(&self, result: Result<Option<Relation>, String>) {
        let &(ref lock, ref cvar) = &*self.result;
        let mut old_result = lock.lock().unwrap();
        old_result.replace(result);
        cvar.notify_one();
    }

    pub fn await_result(&self) -> Result<Option<Relation>, String> {
        let &(ref lock, ref cvar) = &*self.result;
        let mut result = lock.lock().unwrap();
        loop {
            if let Some(r) = result.take() {
                break r;
            }
            result = cvar.wait(result).unwrap();
        }
    }
}

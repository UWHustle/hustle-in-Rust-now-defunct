use std::sync::{Mutex, Arc, Condvar};
use std::collections::VecDeque;
use crossbeam_utils::thread;
use execution::logical_entities::relation::Relation;
use execution::ExecutionEngine;

pub struct TransactionManager {
    execution_engine: Arc<ExecutionEngine>,
    input_queue: (Mutex<VecDeque<Arc<Transaction>>>, Condvar),
    is_polling: Mutex<bool>
}

impl TransactionManager {
    pub fn new(execution_engine: Arc<ExecutionEngine>) -> Self {
        TransactionManager {
            execution_engine,
            input_queue: (Mutex::new(VecDeque::new()), Condvar::new()),
            is_polling: Mutex::new(false)
        }
    }

    pub fn connect(&self) -> TransactionManagerConnection {
        let mut is_polling = self.is_polling.lock().unwrap();
        if !*is_polling {
            self.spawn_transaction_processing_thread();
        }
        *is_polling = true;

        TransactionManagerConnection::new(self)
    }

    fn spawn_transaction_processing_thread(&self) {
        thread::scope(|s| {
            s.spawn(move |_| {
                loop {
                    let transaction = self.await_transaction();

                    // Process the transaction.
                    let statement = transaction.await_statement();
                    let result = self.execution_engine.execute_plan(&statement.plan);
                    statement.set_result(Ok(result));
                }
            });
        }).unwrap();
    }

    fn await_transaction(&self) -> Arc<Transaction> {
        let &(ref lock, ref cvar) = &self.input_queue;
        let mut input_queue = lock.lock().unwrap();

        // Continuously poll the input queue for new transactions.
        loop {
            if let Some(transaction) = input_queue.pop_front() {
                // There is a transaction in the front of the input queue. Process it.
                break transaction;
            }

            // The input queue is empty. Wait for a new transaction.
            input_queue = cvar.wait(input_queue).unwrap();
        }
    }
}

pub struct TransactionManagerConnection<'a> {
    current_transaction: Option<Arc<Transaction>>,
    transaction_manager: &'a TransactionManager
}

impl<'a> TransactionManagerConnection<'a> {
    fn new(transaction_manager: &'a TransactionManager) -> Self {
        TransactionManagerConnection {
            current_transaction: None,
            transaction_manager
        }
    }

    pub fn begin_transaction(&mut self) -> Result<(), String> {
        if self.current_transaction.is_none() {
            let transaction = Arc::new(Transaction::new());
            self.current_transaction = Some(transaction.clone());

            let (ref input_queue, ref cvar) = self.transaction_manager.input_queue;
            input_queue
                .lock()
                .unwrap()
                .push_back(transaction);
            cvar.notify_one();

            Ok(())
        } else {
            Err("Cannot begin a transaction within a transaction".to_string())
        }
    }

    pub fn execute_statement(&mut self, plan: String) -> Result<Option<Relation>, String> {
        let statement = Arc::new(TransactionStatement::new(plan));

        if let Some(transaction) = &self.current_transaction {
            transaction.push_statement(statement.clone());
        } else {
            self.begin_transaction()?;
            self.current_transaction.as_ref().unwrap().push_statement(statement.clone());
            self.commit_transaction()?;
        }

        statement.await_result()
    }

    pub fn commit_transaction(&mut self) -> Result<(), String> {
        if let Some(transaction) = self.current_transaction.take() {
            *transaction.committed.lock().unwrap() = true;
            Ok(())
        } else {
            Err("Cannot commit when no transaction is active".to_string())
        }
    }
}

struct Transaction {
    committed: Mutex<bool>,
    statements: Arc<(Mutex<VecDeque<Arc<TransactionStatement>>>, Condvar)>
}

impl Transaction {
    fn new() -> Self {
        Transaction {
            committed: Mutex::new(false),
            statements: Arc::new((Mutex::new(VecDeque::new()), Condvar::new()))
        }
    }

    fn push_statement(&self, statement: Arc<TransactionStatement>) {
        let &(ref lock, ref cvar) = &*self.statements;
        lock.lock().unwrap().push_back(statement);
        cvar.notify_one();
    }

    fn await_statement(&self) -> Arc<TransactionStatement> {
        let &(ref lock, ref cvar) = &*self.statements;
        let mut statements = lock.lock().unwrap();
        loop {
            if let Some(statement) = statements.pop_front() {
                break statement;
            }
            statements = cvar.wait(statements).unwrap();
        }
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

use std::sync::{Mutex, Arc, Condvar};
use std::collections::VecDeque;
use execution::logical_entities::relation::Relation;

pub struct TransactionManager {
    input_queue: Mutex<VecDeque<Arc<Mutex<Transaction>>>>
}

impl TransactionManager {
    pub fn new() -> Self {
        TransactionManager {
            input_queue: Mutex::new(VecDeque::new())
        }
    }

    pub fn connect(&self) -> TransactionManagerConnection {
        TransactionManagerConnection::new(self)
    }
}

pub struct TransactionManagerConnection<'a> {
    current_transaction: Option<Arc<Mutex<Transaction>>>,
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
            self.current_transaction = Some(Arc::new(Mutex::new(Transaction::new())));
            self.transaction_manager.input_queue
                .lock()
                .unwrap()
                .push_back(self.current_transaction.as_ref().unwrap().clone());
            Ok(())
        } else {
            Err("Cannot begin a transaction within a transaction".to_string())
        }
    }

    pub fn execute_statement(&mut self, plan: String) -> Result<Option<Relation>, String> {
        let statement_cvar = Arc::new((
            Mutex::new(TransactionStatement::new(plan)),
            Condvar::new()
        ));

        if let Some(transaction) = &self.current_transaction {
            transaction.lock().unwrap().statements.push(statement_cvar.clone());
        } else {
            self.begin_transaction()?;
            self.current_transaction.as_ref().unwrap().lock().unwrap().statements
                .push(statement_cvar.clone());
            self.commit_transaction()?;
        }

        let &(ref lock, ref cvar) = &*statement_cvar;
        let mut statement = lock.lock().unwrap();
        while !statement.completed {
            statement = cvar.wait(statement).unwrap();
        }

        statement.result.to_owned()
    }

    pub fn commit_transaction(&mut self) -> Result<(), String> {
        if let Some(transaction) = self.current_transaction.take() {
            transaction.lock().unwrap().committed = true;
            Ok(())
        } else {
            Err("Cannot commit when no transaction is active".to_string())
        }
    }
}

struct Transaction {
    committed: bool,
    statements: Vec<Arc<(Mutex<TransactionStatement>, Condvar)>>
}

impl Transaction {
    fn new() -> Self {
        Self::with_statements(vec![])
    }

    fn with_statements(statements: Vec<Arc<(Mutex<TransactionStatement>, Condvar)>>) -> Self {
        Transaction {
            statements,
            committed: false
        }
    }
}

struct TransactionStatement {
    plan: String,
    completed: bool,
    result: Result<Option<Relation>, String>
}

impl TransactionStatement {
    pub fn new(plan: String) -> Self {
        TransactionStatement {
            plan,
            completed: false,
            result: Ok(None)
        }
    }
}

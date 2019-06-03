use crate::statement::HustleStatement;
use execution::ExecutionEngine;
use concurrency::{TransactionManagerConnection, TransactionManager};

pub struct HustleConnection<'a> {
    transaction_manager_connection: TransactionManagerConnection<'a>,
    execution_engine: &'a ExecutionEngine
}

impl<'a> HustleConnection<'a> {
    pub fn new(
        transaction_manager: &'a TransactionManager,
        execution_engine: &'a ExecutionEngine
    ) -> Self {
        HustleConnection {
            transaction_manager_connection: transaction_manager.connect(),
            execution_engine
        }
    }

    pub fn begin_transaction(&mut self) -> Result<(), String> {
        self.transaction_manager_connection.begin_transaction()
    }

    pub fn commit_transaction(&mut self) -> Result<(), String> {
        self.transaction_manager_connection.commit_transaction()
    }

    pub fn prepare_statement(&self, sql: &str) -> HustleStatement {
        HustleStatement::new(sql, self)
    }

    pub fn transaction_manager_connection(&self) -> &TransactionManagerConnection {
        &self.transaction_manager_connection
    }

    pub fn execution_engine(&self) -> &ExecutionEngine {
        self.execution_engine
    }
}

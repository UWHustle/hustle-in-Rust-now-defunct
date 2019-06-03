use execution::ExecutionEngine;
use concurrency::TransactionManager;
use crate::HustleConnection;

pub struct Hustle {
    transaction_manager: TransactionManager,
    execution_engine: ExecutionEngine
}

impl Hustle {
    pub fn new() -> Self {
        Hustle {
            transaction_manager: TransactionManager::new(),
            execution_engine: ExecutionEngine::new()
        }
    }

    pub fn connect(&self) -> HustleConnection {
        HustleConnection::new(&self.transaction_manager, &self.execution_engine)
    }
}

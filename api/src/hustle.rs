use execution::ExecutionEngine;
use concurrency::TransactionManager;
use crate::HustleConnection;
use std::sync::Arc;

pub struct Hustle {
    transaction_manager: TransactionManager,
    execution_engine: Arc<ExecutionEngine>
}

impl Hustle {
    pub fn new() -> Self {
        let execution_engine = Arc::new(ExecutionEngine::new());
        Hustle {
            transaction_manager: TransactionManager::new(execution_engine.clone()),
            execution_engine
        }
    }

    pub fn connect(&self) -> HustleConnection {
        HustleConnection::new(&self.transaction_manager, &self.execution_engine)
    }
}

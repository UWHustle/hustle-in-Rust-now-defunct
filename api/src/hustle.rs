use execution::ExecutionEngine;
use crate::HustleConnection;

pub struct Hustle {
    execution_engine: ExecutionEngine
}

impl Hustle {
    pub fn new() -> Self {
        Hustle {
            execution_engine: ExecutionEngine::new()
        }
    }

    pub fn connect(&self) -> HustleConnection {
        HustleConnection::new(&self.execution_engine)
    }
}

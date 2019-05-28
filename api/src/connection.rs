use crate::statement::HustleStatement;
use execution::ExecutionEngine;

pub struct HustleConnection {
    execution_engine: ExecutionEngine
}

impl HustleConnection {
    pub fn new() -> Self {
        HustleConnection {
            execution_engine: ExecutionEngine::new()
        }
    }

    pub fn prepare(&self, sql: &str) -> HustleStatement {
        HustleStatement::new(sql, &self.execution_engine)
    }
}

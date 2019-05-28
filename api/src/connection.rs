use crate::statement::Statement;
use execution::ExecutionEngine;

pub struct Connection {
    execution_engine: ExecutionEngine
}

impl Connection {
    pub fn new() -> Self {
        Connection {
            execution_engine: ExecutionEngine::new()
        }
    }

    pub fn prepare(&self, sql: &str) -> Statement {
        Statement::new(sql, &self.execution_engine)
    }
}

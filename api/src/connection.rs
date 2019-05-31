use crate::statement::HustleStatement;
use execution::ExecutionEngine;

pub struct HustleConnection<'a> {
    execution_engine: &'a ExecutionEngine
}

impl<'a> HustleConnection<'a> {
    pub fn new(execution_engine: &'a ExecutionEngine) -> Self {
        HustleConnection {
            execution_engine
        }
    }

    pub fn prepare(&self, sql: &str) -> HustleStatement {
        HustleStatement::new(sql, &self.execution_engine)
    }
}

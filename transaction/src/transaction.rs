use std::collections::VecDeque;
use message::Statement;

pub struct Transaction {
    pub id: u64,
    pub committed: bool,
    statements: VecDeque<Statement>,
}

impl Transaction {
    pub fn new(id: u64) -> Self {
        Transaction {
            id,
            committed: false,
            statements: VecDeque::new(),
        }
    }

    pub fn enqueue_statement(&mut self, statement: Statement) {
        self.statements.push_back(statement);
    }

    pub fn dequeue_statement(&mut self) -> Option<Statement> {
        self.statements.pop_front()
    }

    pub fn commit(&mut self) {
        self.committed = true;
    }

    pub fn is_empty(&self) -> bool {
        self.statements.is_empty()
    }
}
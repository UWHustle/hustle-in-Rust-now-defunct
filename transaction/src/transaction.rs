use std::collections::VecDeque;
use crate::statement::Statement;

pub struct Transaction {
    pub id: u64,
    pub committed: bool,
    pub statements: VecDeque<Statement>,
}

impl Transaction {
    pub fn new(id: u64) -> Self {
        Transaction {
            id,
            committed: false,
            statements: VecDeque::new(),
        }
    }
}

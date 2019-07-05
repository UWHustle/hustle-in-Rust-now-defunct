use std::collections::VecDeque;
use crate::transaction::Transaction;

pub struct DirectPredicatePolicy {
    running_statements: VecDeque<Transaction>,
    sidetracked_statements: VecDeque<Transaction>,
}

impl DirectPredicatePolicy {
    pub fn new() -> Self {
        DirectPredicatePolicy {
            running_statements: VecDeque::new(),
            sidetracked_statements: VecDeque::new(),
        }
    }
}

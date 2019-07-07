use std::collections::VecDeque;

use message::Statement;

use crate::policy::Policy;
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

impl Policy for DirectPredicatePolicy {
    fn begin_transaction(&mut self, transaction: Transaction) {
        unimplemented!()
    }

    fn commit_transaction(&mut self, transaction_id: u64) -> Vec<Statement> {
        unimplemented!()
    }

    fn enqueue_statement(&mut self, statement: Statement) -> Vec<Statement> {
        unimplemented!()
    }

    fn complete_statement(&mut self, statement: Statement) -> Vec<Statement> {
        unimplemented!()
    }
}

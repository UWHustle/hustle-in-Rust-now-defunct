use std::collections::{HashSet, VecDeque};

use message::Plan;

use crate::{lock::PredicateLock, policy::Policy, Transaction};

pub struct DirectPredicatePolicy {
    predicate_locks: HashSet<PredicateLock>,
    sidetracked_statements: VecDeque<Transaction>,
}

impl DirectPredicatePolicy {
    pub fn new() -> Self {
        DirectPredicatePolicy {
            predicate_locks: HashSet::new(),
            sidetracked_statements: VecDeque::new(),
        }
    }
}

impl Policy for DirectPredicatePolicy {
    fn begin_transaction(&mut self) -> u64 {
        unimplemented!()
    }

    fn commit_transaction(&mut self, transaction_id: u64, callback: &Fn(Plan, u64)) {
        unimplemented!()
    }

    fn enqueue_statement(&mut self, transaction_id: u64, plan: Plan, callback: &Fn(Plan, u64)) {
        unimplemented!()
    }

    fn complete_statement(&mut self, statement_id: u64, callback: &Fn(Plan, u64)) {
        unimplemented!()
    }
}

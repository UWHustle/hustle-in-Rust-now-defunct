use std::collections::VecDeque;

pub use direct_predicate::DirectPredicatePolicy;
use hustle_common::Plan;
pub use zero_concurrency::ZeroConcurrencyPolicy;

use crate::statement::Statement;
use crate::transaction::Transaction;

mod zero_concurrency;
mod direct_predicate;

pub trait Policy {
    fn begin_transaction(&mut self) -> u64;
    fn commit_transaction(&mut self, transaction_id: u64, callback: &Fn(Plan, u64));
    fn enqueue_statement(&mut self, transaction_id: u64, plan: Plan, callback: &Fn(Plan, u64));
    fn complete_statement(&mut self, statement_id: u64, callback: &Fn(Plan, u64));
}

pub struct PolicyHelper {
    sidetracked: VecDeque<Transaction>,
    transaction_ctr: u64,
    statement_ctr: u64,
}

impl PolicyHelper {
    pub fn new() -> Self {
        PolicyHelper {
            sidetracked: VecDeque::new(),
            transaction_ctr: 0,
            statement_ctr: 0,
        }
    }

    pub fn new_transaction(&mut self) -> u64 {
        let transaction_id = self.transaction_ctr;
        self.transaction_ctr = self.transaction_ctr.wrapping_add(1);
        self.sidetracked.push_back(Transaction::new(transaction_id));
        transaction_id
    }

    pub fn new_statement(&mut self, transaction_id: u64, plan: Plan) -> Statement {
        let statement_id = self.statement_ctr;
        self.statement_ctr = self.statement_ctr.wrapping_add(1);
        Statement::new(statement_id, transaction_id, plan)
    }

    pub fn enqueue_statement(&mut self, statement: Statement) {
        self.get_transaction_mut(statement.transaction_id).statements.push_back(statement);
    }

    pub fn sidetracked(&self) -> &VecDeque<Transaction> {
        &self.sidetracked
    }

    pub fn sidetracked_mut(&mut self) -> &mut VecDeque<Transaction> {
        &mut self.sidetracked
    }

    pub fn get_transaction(&self, transaction_id: u64) -> &Transaction {
        // TODO: Index the transactions on transaction id.
        self.sidetracked.iter()
            .find(|t| t.id == transaction_id)
            .unwrap()
    }

    pub fn get_transaction_mut(&mut self, transaction_id: u64) -> &mut Transaction {
        // TODO: Index the transactions on transaction id.
        self.sidetracked.iter_mut()
            .find(|t| t.id == transaction_id)
            .unwrap()
    }
}

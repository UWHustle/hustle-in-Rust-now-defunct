pub use direct_predicate::DirectPredicatePolicy;
use message::Plan;
pub use zero_concurrency::ZeroConcurrencyPolicy;

use crate::{Statement, Transaction, TransactionQueue};

mod zero_concurrency;
mod direct_predicate;

pub trait Policy {
    fn begin_transaction(&mut self) -> u64;
    fn commit_transaction(&mut self, transaction_id: u64, callback: &Fn(Plan, u64));
    fn enqueue_statement(&mut self, transaction_id: u64, plan: Plan, callback: &Fn(Plan, u64));
    fn complete_statement(&mut self, statement_id: u64, callback: &Fn(Plan, u64));
}

pub struct BasePolicy {
    transaction_queue: TransactionQueue,
    transaction_ctr: u64,
    statement_ctr: u64,
}

impl BasePolicy {
    pub fn new() -> Self {
        BasePolicy {
            transaction_queue: TransactionQueue::new(),
            transaction_ctr: 0,
            statement_ctr: 0,
        }
    }

    fn get_transaction(&mut self, transaction_id: u64) -> &mut Transaction {
        self.transaction_queue.get_mut(&transaction_id)
            .expect(&format!("Transaction id {} not found in sidetrack queue", transaction_id))
    }
}

impl Policy for BasePolicy {
    fn begin_transaction(&mut self) -> u64 {
        let transaction_id = self.transaction_ctr;
        self.transaction_ctr = self.transaction_ctr.wrapping_add(1);
        self.transaction_queue.push_back(Transaction::new(transaction_id));
        transaction_id
    }

    fn commit_transaction(&mut self, transaction_id: u64, callback: &Fn(Plan, u64)) {
        self.get_transaction(transaction_id).commit()
    }

    fn enqueue_statement(&mut self, transaction_id: u64, plan: Plan, callback: &Fn(Plan, u64)) {
        let statement_id = self.statement_ctr;
        self.statement_ctr = self.statement_ctr.wrapping_add(1);
        self.get_transaction(transaction_id).push_back(Statement::new(statement_id, plan));
    }

    fn complete_statement(&mut self, statement_id: u64, callback: &Fn(Plan, u64)) { }
}

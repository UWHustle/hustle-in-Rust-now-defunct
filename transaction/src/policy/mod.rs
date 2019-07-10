pub use direct_predicate::DirectPredicatePolicy;
use message::Plan;
pub use zero_concurrency::ZeroConcurrencyPolicy;

mod zero_concurrency;
mod direct_predicate;

pub trait Policy {
    fn begin_transaction(&mut self) -> u64;
    fn commit_transaction(&mut self, transaction_id: u64, callback: &Fn(Plan, u64));
    fn enqueue_statement(&mut self, transaction_id: u64, plan: Plan, callback: &Fn(Plan, u64));
    fn complete_statement(&mut self, statement_id: u64, callback: &Fn(Plan, u64));
}

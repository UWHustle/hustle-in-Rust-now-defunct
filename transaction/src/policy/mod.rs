pub use zero_concurrency::ZeroConcurrencyPolicy;
use message::Statement;
use crate::transaction::Transaction;

mod zero_concurrency;

pub trait Policy {
    fn begin_transaction(&mut self, transaction: Transaction);
    fn commit_transaction(&mut self, transaction_id: u64) -> Vec<Statement>;
    fn enqueue_statement(&mut self, statement: Statement) -> Vec<Statement>;
    fn complete_statement(&mut self, statement: Statement) -> Vec<Statement>;
}

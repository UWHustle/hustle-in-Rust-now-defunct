use std::collections::VecDeque;
use crate::policy::Policy;
use message::Statement;
use crate::transaction::Transaction;

pub struct ZeroConcurrencyPolicy {
    running_statement: bool,
    transaction_queue: VecDeque<Transaction>,
}

impl ZeroConcurrencyPolicy {
    pub fn new() -> Self {
        ZeroConcurrencyPolicy {
            running_statement: false,
            transaction_queue: VecDeque::new(),
        }
    }

    /// Returns the `Transaction` with the given `transaction_id`. A `panic!` will occur if it is
    /// not found.
    fn find(&mut self, transaction_id: u64) -> &mut Transaction {
        self.transaction_queue.iter_mut()
            .find(|t| t.id == transaction_id)
            .expect(&format!("Transaction id {} not found in sidetrack queue", transaction_id))
    }

    /// Returns a vector of `Statement`s that can be safely admitted to the execution engine.
    fn admit_statements(&mut self) -> Vec<Statement> {
        // Remove committed and empty statements from the queue.
        while self.transaction_queue.front()
            .map(|t| t.committed && t.is_empty())
            .unwrap_or(false)
        {
            self.transaction_queue.pop_front();
        }

        // Return statements that can be safely admitted to the execution engine.
        if self.running_statement {
            // There is currently a statement running. No statements can be executed at this time.
            vec![]
        } else {
            if let Some(next_statement) = self.transaction_queue.front_mut()
                .and_then(|t| t.dequeue_statement())
            {
                // The front transaction has a statement to execute.
                self.running_statement = true;
                vec![next_statement]
            } else {
                // The front transaction has no statements to execute at this time.
                vec![]
            }
        }
    }
}

impl Policy for ZeroConcurrencyPolicy {
    fn begin_transaction(&mut self, transaction: Transaction) {
        self.transaction_queue.push_back(transaction);
    }

    fn commit_transaction(&mut self, transaction_id: u64) -> Vec<Statement> {
        let transaction = self.find(transaction_id);
        transaction.commit();
        self.admit_statements()
    }

    fn enqueue_statement(&mut self, transaction_id: u64, statement: Statement) -> Vec<Statement> {
        let transaction = self.find(transaction_id);
        transaction.enqueue_statement(statement);
        self.admit_statements()
    }

    fn complete_statement(&mut self, _transaction_id: u64, _statement: Statement) -> Vec<Statement> {
        self.running_statement = false;
        self.admit_statements()
    }
}

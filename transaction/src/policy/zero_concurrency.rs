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

    fn enqueue_statement(&mut self, statement: Statement) -> Vec<Statement> {
        let transaction = self.find(statement.transaction_id);
        transaction.enqueue_statement(statement);
        self.admit_statements()
    }

    fn complete_statement(&mut self, _statement: Statement) -> Vec<Statement> {
        self.running_statement = false;
        self.admit_statements()
    }
}

#[cfg(test)]
mod zero_concurrency_policy_tests {
    use super::*;
    use message::{Plan, Table};

    #[test]
    fn single_connection_test() {
        // Initialize the policy.
        let mut policy = ZeroConcurrencyPolicy::new();

        assert!(!policy.running_statement);
        assert!(policy.transaction_queue.is_empty());

        // Begin a transaction.
        let transaction = Transaction::new(0);
        let transaction_id = transaction.id;
        policy.begin_transaction(transaction);

        assert_eq!(policy.transaction_queue.len(), 1);

        // Enqueue the first statement in the transaction.
        let plan = Plan::TableReference { table: Table::new("table".to_owned(), vec![]) };
        let first_statement = Statement::new(0, transaction_id, 0, plan.clone());
        let mut statements = policy.enqueue_statement(first_statement);

        assert!(policy.running_statement);
        assert_eq!(statements.get(0).map(|s| s.id), Some(0));

        // Enqueue the second statement in the transaction.
        let completed_statement = statements.pop().unwrap();
        let second_statement = Statement::new(1, transaction_id, 0, plan);
        let statements = policy.enqueue_statement(second_statement);

        assert!(statements.is_empty());

        // Complete the first statement.
        let mut statements = policy.complete_statement(completed_statement);

        assert_eq!(statements.get(0).map(|s| s.id), Some(1));

        // Complete the second statement.
        let completed_statement = statements.pop().unwrap();
        let statements = policy.complete_statement(completed_statement);

        assert!(!policy.running_statement);
        assert!(statements.is_empty());

        // Commit the transaction.
        let statements = policy.commit_transaction(transaction_id);

        assert!(policy.transaction_queue.is_empty());
        assert!(statements.is_empty());
    }

    #[test]
    fn multiple_connection_test() {
        // Initialize the policy.
        let mut policy = ZeroConcurrencyPolicy::new();

        assert!(!policy.running_statement);
        assert!(policy.transaction_queue.is_empty());

        // Begin the first transaction.
        let first_transaction = Transaction::new(0);
        let first_transaction_id = first_transaction.id;
        policy.begin_transaction(first_transaction);

        assert_eq!(policy.transaction_queue.len(), 1);

        // Begin the second transaction.
        let second_transaction = Transaction::new(1);
        let second_transaction_id = second_transaction.id;
        policy.begin_transaction(second_transaction);

        assert_eq!(policy.transaction_queue.len(), 2);

        // Enqueue the first statement in the first transaction.
        let plan = Plan::TableReference { table: Table::new("table".to_owned(), vec![]) };
        let first_statement = Statement::new(0, first_transaction_id, 0, plan.clone());
        let mut statements = policy.enqueue_statement(first_statement);

        assert!(policy.running_statement);
        assert_eq!(statements.get(0).map(|s| s.id), Some(0));

        // Enqueue the second statement in the second transaction.
        let completed_statement = statements.pop().unwrap();
        let second_statement = Statement::new(1, second_transaction_id, 1, plan.clone());
        let statements = policy.enqueue_statement(second_statement);

        assert!(statements.is_empty());

        // Enqueue the third statement in the first transaction.
        let third_statement = Statement::new(2, first_transaction_id, 0, plan.clone());
        let statements = policy.enqueue_statement(third_statement);

        assert!(statements.is_empty());

        // Complete the first statement.
        let mut statements = policy.complete_statement(completed_statement);

        assert_eq!(statements.get(0).map(|s| s.id), Some(2));

        // Complete the third statement.
        let completed_statement = statements.pop().unwrap();
        let statements = policy.complete_statement(completed_statement);

        assert!(statements.is_empty());

        // Commit the first transaction.
        let mut statements = policy.commit_transaction(first_transaction_id);

        assert_eq!(policy.transaction_queue.len(), 1);
        assert_eq!(statements.get(0).map(|s| s.id), Some(1));

        // Complete the second statement.
        let completed_statement = statements.pop().unwrap();
        let statements = policy.complete_statement(completed_statement);

        assert!(!policy.running_statement);
        assert!(statements.is_empty());

        let statements = policy.commit_transaction(second_transaction_id);

        assert!(policy.transaction_queue.is_empty());
        assert!(statements.is_empty());
    }
}

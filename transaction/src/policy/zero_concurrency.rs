use message::Plan;

use crate::{policy::{BasePolicy, Policy}, TransactionQueue};

pub struct ZeroConcurrencyPolicy {
    base_policy: BasePolicy,
    running_statement: bool,
}

impl ZeroConcurrencyPolicy {
    pub fn new() -> Self {
        ZeroConcurrencyPolicy {
            base_policy: BasePolicy::new(),
            running_statement: false,
        }
    }

    /// Returns a vector of `Statement`s that can be safely admitted to the execution engine.
    fn admit_statements(&mut self, callback: &Fn(Plan, u64)) {
        // Remove committed and empty transactions from the queue.
        while self.base_policy.transaction_queue.front()
            .map(|t| t.committed && t.is_empty())
            .unwrap_or(false)
        {
            self.base_policy.transaction_queue.pop_front();
        }

        // Call the callback with statements that can be safely admitted to the execution engine.
        if !self.running_statement {
            if let Some(next_statement) = self.base_policy.transaction_queue.front_mut()
                .and_then(|t| t.pop_front())
            {
                self.running_statement = true;
                callback(next_statement.plan.clone(), next_statement.id)
            }
        }
    }
}

impl Policy for ZeroConcurrencyPolicy {
    fn begin_transaction(&mut self) -> u64 {
        self.base_policy.begin_transaction()
    }

    fn commit_transaction(&mut self, transaction_id: u64, callback: &Fn(Plan, u64)) {
        self.base_policy.commit_transaction(transaction_id, callback);
        self.admit_statements(callback);
    }

    fn enqueue_statement(&mut self, transaction_id: u64, plan: Plan, callback: &Fn(Plan, u64)) {
        self.base_policy.enqueue_statement(transaction_id, plan, callback);
        self.admit_statements(callback);
    }

    fn complete_statement(&mut self, statement_id: u64, callback: &Fn(Plan, u64)) {
        self.base_policy.complete_statement(statement_id, callback);
        self.running_statement = false;
        self.admit_statements(callback);
    }
}

#[cfg(test)]
mod zero_concurrency_policy_tests {
    use super::*;
    use message::{Plan, Table};
    use crate::{Transaction, Statement};
    use std::collections::VecDeque;
    use std::cell::RefCell;

    #[test]
    fn single_connection_test() {
        // Initialize the policy.
        let mut policy = ZeroConcurrencyPolicy::new();

        assert!(!policy.running_statement);
        assert!(policy.base_policy.transaction_queue.is_empty());

        // Begin a transaction.
        let transaction_id = policy.begin_transaction();

        assert_eq!(policy.base_policy.transaction_queue.len(), 1);

        // Enqueue the first statement in the transaction.
        let plan = Plan::TableReference { table: Table::new("table".to_owned(), vec![]) };
        let admitted = RefCell::new(VecDeque::new());
        let callback = |_, statement_id| admitted.borrow_mut().push_back(statement_id);
        policy.enqueue_statement(transaction_id, plan.clone(), &callback);

        assert!(policy.running_statement);
        assert_eq!(admitted.borrow().front(), Some(&0));

        // Enqueue the second statement in the transaction.
        policy.enqueue_statement(transaction_id, plan, &callback);

        assert_eq!(admitted.borrow().len(), 1);

        // Complete the first statement.
        let statement_id = admitted.borrow_mut().pop_front().unwrap();
        policy.complete_statement(statement_id, &callback);

        assert_eq!(admitted.borrow().front(), Some(&1));

        // Complete the second statement.
        let statement_id = admitted.borrow_mut().pop_front().unwrap();
        policy.complete_statement(statement_id, &callback);

        assert!(!policy.running_statement);
        assert!(admitted.borrow().is_empty());

        // Commit the transaction.
        policy.commit_transaction(transaction_id, &callback);

        assert!(policy.base_policy.transaction_queue.is_empty());
        assert!(admitted.borrow().is_empty());
    }

    #[test]
    fn multiple_connection_test() {
        // Initialize the policy.
        let mut policy = ZeroConcurrencyPolicy::new();

        assert!(!policy.running_statement);
        assert!(policy.base_policy.transaction_queue.is_empty());

        // Begin the first transaction.
        let first_transaction_id = policy.begin_transaction();

        assert_eq!(policy.base_policy.transaction_queue.len(), 1);

        // Begin the second transaction.
        let second_transaction_id = policy.begin_transaction();

        assert_eq!(policy.base_policy.transaction_queue.len(), 2);

        // Enqueue the first statement in the first transaction.
        let plan = Plan::TableReference { table: Table::new("table".to_owned(), vec![]) };
        let admitted = RefCell::new(VecDeque::new());
        let callback = |_, statement_id| admitted.borrow_mut().push_back(statement_id);
        policy.enqueue_statement(first_transaction_id, plan.clone(), &callback);

        assert!(policy.running_statement);
        assert_eq!(admitted.borrow().front(), Some(&0));

        // Enqueue the second statement in the second transaction.
        policy.enqueue_statement(second_transaction_id, plan.clone(), &callback);

        assert_eq!(admitted.borrow().len(), 1);

        // Enqueue the third statement in the first transaction.
        policy.enqueue_statement(first_transaction_id, plan, &callback);

        assert_eq!(admitted.borrow().len(), 1);

        // Complete the first statement.
        let statement_id = admitted.borrow_mut().pop_front().unwrap();
        policy.complete_statement(statement_id, &callback);

        assert_eq!(admitted.borrow().front(), Some(&2));

        // Complete the third statement.
        let statement_id = admitted.borrow_mut().pop_front().unwrap();
        policy.complete_statement(statement_id, &callback);

        assert!(admitted.borrow().is_empty());

        // Commit the first transaction.
        policy.commit_transaction(first_transaction_id, &callback);

        assert_eq!(policy.base_policy.transaction_queue.len(), 1);
        assert_eq!(admitted.borrow().front(), Some(&1));

        // Complete the second statement.
        let statement_id = admitted.borrow_mut().pop_front().unwrap();
        policy.complete_statement(statement_id, &callback);

        assert!(!policy.running_statement);
        assert!(admitted.borrow().is_empty());

        policy.commit_transaction(second_transaction_id, &callback);

        assert!(policy.base_policy.transaction_queue.is_empty());
        assert!(admitted.borrow().is_empty());
    }
}

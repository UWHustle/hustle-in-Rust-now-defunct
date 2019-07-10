use std::collections::VecDeque;

use hustle_common::Plan;

use crate::{policy::Policy, Statement, Transaction};

pub struct ZeroConcurrencyPolicy {
    sidetracked: VecDeque<Transaction>,
    running_statement: bool,
    transaction_ctr: u64,
    statement_ctr: u64,
}

impl ZeroConcurrencyPolicy {
    pub fn new() -> Self {
        ZeroConcurrencyPolicy {
            sidetracked: VecDeque::new(),
            running_statement: false,
            transaction_ctr: 0,
            statement_ctr: 0,
        }
    }

    fn find_transaction_mut(&mut self, transaction_id: u64) -> &mut Transaction {
        // TODO: Index the transactions on transaction id.
        self.sidetracked.iter_mut()
            .find(|t| t.id == transaction_id)
            .unwrap()
    }

    fn admit_sidetracked(&mut self, callback: &Fn(Plan, u64)) {
        while self.sidetracked.front()
            .map(|t| t.committed && t.statements.is_empty())
            .unwrap_or(false)
            {
                self.sidetracked.pop_front();
            }

        if !self.running_statement {
            if let Some(statement) = self.sidetracked.front_mut()
                .and_then(|t| t.statements.pop_front())
            {
                self.running_statement = true;
                callback(statement.plan, statement.id)
            }
        }
    }
}

impl Policy for ZeroConcurrencyPolicy {
    fn begin_transaction(&mut self) -> u64 {
        let transaction_id = self.transaction_ctr;
        self.transaction_ctr = self.transaction_ctr.wrapping_add(1);
        let transaction = Transaction::new(transaction_id);
        self.sidetracked.push_back(transaction);
        transaction_id
    }

    fn commit_transaction(&mut self, transaction_id: u64, callback: &Fn(Plan, u64)) {
        let committed_transaction = self.find_transaction_mut(transaction_id);
        committed_transaction.committed = true;
        if committed_transaction.statements.is_empty() {
            // TODO: Remove from sidetrack
            self.admit_sidetracked(callback);
        }
    }

    fn enqueue_statement(&mut self, transaction_id: u64, plan: Plan, callback: &Fn(Plan, u64)) {
        let statement_id = self.statement_ctr;
        self.statement_ctr = self.statement_ctr.wrapping_add(1);
        let statement = Statement::new(statement_id, transaction_id, plan);

        if !self.running_statement && self.sidetracked.front().unwrap().id == transaction_id {
            let plan = statement.plan.clone();
            self.running_statement = true;
            callback(plan, statement_id);
        } else {
            self.find_transaction_mut(transaction_id).statements.push_back(statement);
        }
    }

    fn complete_statement(&mut self, _statement_id: u64, callback: &Fn(Plan, u64)) {
        self.running_statement = false;
        self.admit_sidetracked(callback);
    }
}

#[cfg(test)]
mod zero_concurrency_policy_tests {
    use std::cell::RefCell;
    use std::collections::VecDeque;

    use hustle_common::{Plan, Table};

    use super::*;

    #[test]
    fn single_connection_test() {
        // Initialize the policy.
        let mut policy = ZeroConcurrencyPolicy::new();

        assert!(!policy.running_statement);
        assert!(policy.sidetracked.is_empty());

        // Begin a transaction.
        let transaction_id = policy.begin_transaction();

        assert_eq!(policy.sidetracked.len(), 1);

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

        assert!(policy.sidetracked.is_empty());
        assert!(admitted.borrow().is_empty());
    }

    #[test]
    fn multiple_connection_test() {
        // Initialize the policy.
        let mut policy = ZeroConcurrencyPolicy::new();

        assert!(!policy.running_statement);
        assert!(policy.sidetracked.is_empty());

        // Begin the first transaction.
        let first_transaction_id = policy.begin_transaction();

        assert_eq!(policy.sidetracked.len(), 1);

        // Begin the second transaction.
        let second_transaction_id = policy.begin_transaction();

        assert_eq!(policy.sidetracked.len(), 2);

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

        assert_eq!(policy.sidetracked.len(), 1);
        assert_eq!(admitted.borrow().front(), Some(&1));

        // Complete the second statement.
        let statement_id = admitted.borrow_mut().pop_front().unwrap();
        policy.complete_statement(statement_id, &callback);

        assert!(!policy.running_statement);
        assert!(admitted.borrow().is_empty());

        policy.commit_transaction(second_transaction_id, &callback);

        assert!(policy.sidetracked.is_empty());
        assert!(admitted.borrow().is_empty());
    }
}

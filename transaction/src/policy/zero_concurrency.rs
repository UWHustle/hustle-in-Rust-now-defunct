use std::collections::VecDeque;

use hustle_common::plan::{Plan, Statement};

use crate::policy::Policy;

pub struct ZeroConcurrencyPolicy {
    running_statement: Option<Statement>,
    active_transaction_id: Option<u64>,
    sidetracked_statements: VecDeque<Statement>,
}

impl ZeroConcurrencyPolicy {
    pub fn new() -> Self {
        ZeroConcurrencyPolicy {
            running_statement: None,
            active_transaction_id: None,
            sidetracked_statements: VecDeque::new(),
        }
    }

    fn admit_front(&mut self) -> Vec<Statement> {
        // This should only be called when no statement is running and no transaction is active.
        assert!(self.running_statement.is_none());
        assert!(self.active_transaction_id.is_none());

        if let Some(sidetracked_statement) = self.sidetracked_statements.pop_front() {
            // The sidetracked statement must be a begin transaction statement.
            assert_eq!(
                sidetracked_statement.plan,
                Plan::BeginTransaction,
                "Transaction not found for statement with ID {}",
                sidetracked_statement.id,
            );

            // Admit the sidetracked statement.
            self.running_statement = Some(sidetracked_statement.clone());
            self.active_transaction_id = Some(sidetracked_statement.transaction_id);
            vec![sidetracked_statement]
        } else {
            vec![]
        }
    }
}

impl Policy for ZeroConcurrencyPolicy {
    fn enqueue_statement(&mut self, statement: Statement) -> Vec<Statement> {
        if self.running_statement.is_some() {
            // A statement is currently running. Sidetrack this statement to admit later.
            self.sidetracked_statements.push_back(statement);
            vec![]
        } else {
            // No statement is currently running. Check the active transaction ID to determine
            // whether this statement can be admitted.
            if let Some(active_transaction_id) = self.active_transaction_id {
                if statement.transaction_id == active_transaction_id {
                    // The statement is part of the active transaction. It can be admitted
                    // immediately.
                    self.running_statement = Some(statement.clone());
                    vec![statement]
                } else {
                    // The statement is not part of the active transaction. Sidetrack it to admit
                    // later.
                    self.sidetracked_statements.push_back(statement);
                    vec![]
                }
            } else {
                // No transaction is currently active. Sidetrack this statement, then admit from the
                // front of the sidetracked statements.
                self.sidetracked_statements.push_back(statement);
                self.admit_front()
            }
        }
    }

    fn complete_statement(&mut self, statement: Statement) -> Vec<Statement> {
        // Ensure the running statement has the correct statement ID.
        let completed_statement = self.running_statement.take()
            .filter(|s| s.id == statement.id)
            .expect(&format!("Statement with ID {} was not running", statement.id));

        if completed_statement.plan == Plan::CommitTransaction {
            // The statement is a commit transaction statement. Remove the active transaction ID.
            self.active_transaction_id.take()
                .expect("Cannot commit when no transaction is active");
        }

        // Examine the sidetrack to find the next statement to admit.
        if let Some(active_transaction_id) = self.active_transaction_id {
            // There is currently an active transaction. Check if the sidetrack contains a statement
            // belonging to this transaction.
            if let Some(sidetracked_statement) = self.sidetracked_statements.iter()
                .position(|s| s.transaction_id == active_transaction_id)
                .and_then(|p| self.sidetracked_statements.remove(p))
            {
                // The sidetracked statement is part of the active transaction. Remove it from the
                // sidetrack and admit it.
                self.running_statement = Some(sidetracked_statement.clone());
                vec![sidetracked_statement]
            } else {
                // No statements on the sidetrack are part of the active transaction.
                vec![]
            }
        } else {
            // No transaction is currently active. Admit from the front of the sidetracked
            // statements.
            self.admit_front()
        }
    }
}

#[cfg(test)]
mod zero_concurrency_policy_tests {
    use hustle_transaction_test_util as test_util;

    use super::*;

    #[test]
    fn single_connection() {
        // Initialize the policy.
        let mut policy = ZeroConcurrencyPolicy::new();

        assert_eq!(policy.running_statement, None);
        assert_eq!(policy.active_transaction_id, None);
        assert!(policy.sidetracked_statements.is_empty());

        let mut admitted = VecDeque::new();

        // Enqueue the begin transaction statement.
        admitted.extend(policy.enqueue_statement(Statement::new(0, 0, Plan::BeginTransaction)));

        assert_eq!(policy.running_statement.as_ref(), admitted.front());
        assert_eq!(policy.active_transaction_id, Some(0));
        assert_eq!(admitted.len(), 1);

        // Enqueue the second statement in the transaction.
        admitted.extend(policy.enqueue_statement(Statement::new(
            1,
            0,
            test_util::generate_plan("SELECT a FROM T"),
        )));

        assert_eq!(policy.sidetracked_statements.len(), 1);
        assert_eq!(admitted.len(), 1);

        // Enqueue the commit transaction statement.
        admitted.extend(policy.enqueue_statement(Statement::new(2, 0, Plan::CommitTransaction)));

        assert_eq!(policy.sidetracked_statements.len(), 2);
        assert_eq!(admitted.len(), 1);

        // Complete the begin transaction statement.
        let begin = admitted.pop_front().unwrap();
        admitted.extend(policy.complete_statement(begin));

        assert_eq!(policy.sidetracked_statements.len(), 1);
        assert_eq!(admitted.len(), 1);

        // Complete the second statement.
        let statement = admitted.pop_front().unwrap();
        admitted.extend(policy.complete_statement(statement));

        assert!(policy.sidetracked_statements.is_empty());
        assert_eq!(admitted.len(), 1);

        // Complete the commit transaction statement.
        let commit = admitted.pop_front().unwrap();
        admitted.extend(policy.complete_statement(commit));

        assert_eq!(policy.running_statement, None);
        assert_eq!(policy.active_transaction_id, None);
        assert!(admitted.is_empty());
    }

    #[test]
    fn multiple_connection() {
        // Initialize the policy.
        let mut policy = ZeroConcurrencyPolicy::new();

        assert_eq!(policy.running_statement, None);
        assert_eq!(policy.active_transaction_id, None);
        assert!(policy.sidetracked_statements.is_empty());

        let mut admitted = VecDeque::new();

        // Enqueue the first begin transaction statement.
        admitted.extend(policy.enqueue_statement(Statement::new(0, 0, Plan::BeginTransaction)));

        assert_eq!(policy.active_transaction_id, Some(0));
        assert_eq!(admitted.len(), 1);

        // Complete the first begin transaction statement.
        let begin = admitted.pop_front().unwrap();
        admitted.extend(policy.complete_statement(begin));

        assert!(admitted.is_empty());

        // Enqueue the second begin transaction statement.
        admitted.extend(policy.enqueue_statement(Statement::new(1, 1, Plan::BeginTransaction)));

        assert_eq!(policy.sidetracked_statements.len(), 1);
        assert!(admitted.is_empty());

        // Enqueue the second statement in the second transaction.
        admitted.extend(policy.enqueue_statement(Statement::new(
            2,
            1,
            test_util::generate_plan("SELECT a FROM T"),
        )));

        assert_eq!(policy.sidetracked_statements.len(), 2);
        assert!(admitted.is_empty());

        // Enqueue the commit transaction statement in the first transaction.
        admitted.extend(policy.enqueue_statement(Statement::new(3, 0, Plan::CommitTransaction)));

        assert_eq!(policy.running_statement.as_ref(), admitted.front());
        assert_eq!(admitted.len(), 1);

        // Complete the commit transaction statement in the first transaction.
        let commit = admitted.pop_front().unwrap();
        admitted.extend(policy.complete_statement(commit));

        assert_eq!(policy.running_statement.as_ref(), admitted.front());
        assert_eq!(admitted.len(), 1);

        // Complete the begin transaction statement in the second transaction.
        let begin = admitted.pop_front().unwrap();
        admitted.extend(policy.complete_statement(begin));

        assert_eq!(policy.running_statement.as_ref(), admitted.front());
        assert_eq!(admitted.len(), 1);

        // Complete the second statement in the second transaction.
        let statement = admitted.pop_front().unwrap();
        admitted.extend(policy.complete_statement(statement));

        assert_eq!(policy.running_statement.as_ref(), admitted.front());
        assert!(admitted.is_empty());

        // Enqueue the commit transaction statement in the second transaction.
        admitted.extend(policy.enqueue_statement(Statement::new(4, 1, Plan::CommitTransaction)));

        assert_eq!(policy.running_statement.as_ref(), admitted.front());
        assert_eq!(admitted.len(), 1);

        // Complete the commit transaction statement in the second transaction.
        let commit = admitted.pop_front().unwrap();
        admitted.extend(policy.complete_statement(commit));

        assert_eq!(policy.running_statement, None);
        assert_eq!(policy.active_transaction_id, None);
        assert!(admitted.is_empty());
    }
}

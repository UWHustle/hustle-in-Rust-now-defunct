use std::collections::{HashMap, VecDeque};

use hustle_common::plan::{Plan, Statement};

use crate::policy::{ColumnManager, Policy};
use crate::statement::StatementDomain;

pub struct PredicateComparisonPolicy {
    running_statements: HashMap<u64, StatementDomain>,
    completed_statements: HashMap<u64, Vec<StatementDomain>>,
    sidetracked_statements: VecDeque<StatementDomain>,
    column_manager: ColumnManager,
}

impl PredicateComparisonPolicy {
    pub fn new() -> Self {
        PredicateComparisonPolicy {
            running_statements: HashMap::new(),
            completed_statements: HashMap::new(),
            sidetracked_statements: VecDeque::new(),
            column_manager: ColumnManager::new(),
        }
    }

    fn succeeds_statement_in_sidetrack(&self, statement: &StatementDomain) -> bool {
        self.sidetracked_statements.iter()
            .take_while(|sidetracked_statement|
                statement.inner.id != sidetracked_statement.inner.id
            )
            .any(|sidetracked_statement|
                statement.inner.transaction_id == sidetracked_statement.inner.transaction_id
            )
    }

    fn conflicts_with_running_statement(&self, statement: &StatementDomain) -> bool {
        self.running_statements.iter().any(|(_, running_statement)|
            statement.conflicts(running_statement)
        )
    }

    fn conflicts_with_completed_statement(&self, statement: &StatementDomain) -> bool {
        self.completed_statements.iter()
            .filter(|&(&completed_transaction_id, _)|
                statement.inner.transaction_id != completed_transaction_id
            )
            .any(|(_, completed_statements)|
                completed_statements.iter().any(|completed_statement|
                    statement.conflicts(completed_statement)
                )
            )
    }

    fn safe_to_admit(&self, statement: &StatementDomain) -> bool {
        !self.succeeds_statement_in_sidetrack(statement)
            && !self.conflicts_with_running_statement(statement)
            && !self.conflicts_with_completed_statement(statement)
    }
}

impl Policy for PredicateComparisonPolicy {
    fn enqueue_statement(&mut self, statement: Statement) -> Vec<Statement> {
        let enqueued_statement = StatementDomain::from_statement(
            statement.clone(),
            &mut self.column_manager
        );

        if self.safe_to_admit(&enqueued_statement) {
            self.running_statements.insert(enqueued_statement.inner.id, enqueued_statement);
            if !self.completed_statements.contains_key(&statement.transaction_id) {
                assert_eq!(
                    statement.plan,
                    Plan::BeginTransaction,
                    "Transaction not found for statement with ID {}",
                    statement.id,
                );
                self.completed_statements.insert(statement.transaction_id, vec![]);
            }
            vec![statement]
        } else {
            self.sidetracked_statements.push_back(enqueued_statement);
            vec![]
        }
    }

    fn complete_statement(&mut self, statement: Statement) -> Vec<Statement> {
        // Ensure there is a running statement with the correct statement ID.
        let completed_statement = self.running_statements.remove(&statement.id)
            .expect(&format!("Statement with ID {} was not running", statement.id));

        if completed_statement.inner.plan == Plan::CommitTransaction {
            // The statement is a commit transaction statement. Remove the completed statements with
            // this transaction ID.
            self.completed_statements.remove(&statement.transaction_id)
                .expect("Cannot commit when no transaction is active");
        } else {
            // Push the statement to the completed statements vector.
            self.completed_statements.get_mut(&statement.transaction_id)
                .expect(&format!("Transaction not found for statement with ID {}", statement.id))
                .push(completed_statement)
        }

        // Examine the sidetrack to find the next statements to admit.
        // TODO: Convert this logic to drain_filter when it becomes stable.
        let mut statements = vec![];
        let mut statement_i = 0;
        while statement_i != self.sidetracked_statements.len() {
            if self.safe_to_admit(&self.sidetracked_statements[statement_i]) {
                let statement = self.sidetracked_statements.remove(statement_i).unwrap();
                if !self.completed_statements.contains_key(&statement.inner.transaction_id) {
                    assert_eq!(
                        statement.inner.plan,
                        Plan::BeginTransaction,
                        "Transaction not found for statement with ID {}",
                        statement.inner.id,
                    );
                    self.completed_statements.insert(statement.inner.transaction_id, vec![]);
                }
                statements.push(statement.inner.clone());
                self.running_statements.insert(statement.inner.id, statement);
            } else {
                statement_i += 1;
            }
        }

        statements
    }
}

#[cfg(test)]
mod predicate_comparison_policy_tests {
    use hustle_transaction_test_util as test_util;

    use super::*;

    #[test]
    fn single_connection() {
        // Initialize the policy.
        let mut policy = PredicateComparisonPolicy::new();

        assert!(policy.running_statements.is_empty());
        assert!(policy.completed_statements.is_empty());
        assert!(policy.sidetracked_statements.is_empty());

        let mut admitted = VecDeque::new();

        // Enqueue the begin transaction statement.
        admitted.extend(policy.enqueue_statement(Statement::new(0, 0, Plan::BeginTransaction)));

        assert_eq!(policy.running_statements.len(), 1);
        assert!(policy.completed_statements[&0].is_empty());
        assert_eq!(admitted.len(), 1);

        // Enqueue the second statement in the transaction.
        admitted.extend(policy.enqueue_statement(Statement::new(
            1,
            0,
            test_util::generate_plan("SELECT a FROM T"),
        )));

        assert_eq!(policy.running_statements.len(), 2);
        assert_eq!(admitted.len(), 2);

        // Enqueue the commit transaction statement.
        admitted.extend(policy.enqueue_statement(Statement::new(2, 0, Plan::CommitTransaction)));

        assert_eq!(policy.running_statements.len(), 3);
        assert_eq!(admitted.len(), 3);

        // Complete the begin transaction statement.
        let begin = admitted.pop_front().unwrap();
        admitted.extend(policy.complete_statement(begin));

        assert_eq!(policy.running_statements.len(), 2);
        assert_eq!(policy.completed_statements[&0].len(), 1);
        assert_eq!(admitted.len(), 2);

        // Complete the second statement.
        let statement = admitted.pop_front().unwrap();
        admitted.extend(policy.complete_statement(statement));

        assert_eq!(policy.running_statements.len(), 1);
        assert_eq!(policy.completed_statements[&0].len(), 2);
        assert_eq!(admitted.len(), 1);

        // Complete the commit transaction statement.
        let commit = admitted.pop_front().unwrap();
        admitted.extend(policy.complete_statement(commit));

        assert!(policy.running_statements.is_empty());
        assert!(policy.completed_statements.is_empty());
        assert!(admitted.is_empty());
    }

    #[test]
    fn multiple_connection() {
        // Initialize the policy.
        let mut policy = PredicateComparisonPolicy::new();

        assert!(policy.running_statements.is_empty());
        assert!(policy.completed_statements.is_empty());
        assert!(policy.sidetracked_statements.is_empty());

        let mut admitted = VecDeque::new();

        // Enqueue the first begin transaction statement.
        admitted.extend(policy.enqueue_statement(Statement::new(0, 0, Plan::BeginTransaction)));

        assert_eq!(policy.running_statements.len(), 1);
        assert!(policy.completed_statements[&0].is_empty());
        assert_eq!(admitted.len(), 1);

        // Complete the first begin transaction statement.
        let begin = admitted.pop_front().unwrap();
        admitted.extend(policy.complete_statement(begin));

        assert!(policy.running_statements.is_empty());
        assert_eq!(policy.completed_statements[&0].len(), 1);
        assert!(admitted.is_empty());

        // Enqueue the second begin transaction statement.
        admitted.extend(policy.enqueue_statement(Statement::new(1, 1, Plan::BeginTransaction)));

        assert_eq!(policy.running_statements.len(), 1);
        assert!(policy.completed_statements[&1].is_empty());
        assert_eq!(admitted.len(), 1);

        // Complete the second begin transaction statement.
        let begin = admitted.pop_front().unwrap();
        admitted.extend(policy.complete_statement(begin));

        assert!(policy.running_statements.is_empty());
        assert_eq!(policy.completed_statements[&1].len(), 1);
        assert!(admitted.is_empty());

        // Enqueue the second statement in the first transaction.
        admitted.extend(policy.enqueue_statement(Statement::new(
            2,
            0,
            test_util::generate_plan("SELECT a FROM T"),
        )));

        assert_eq!(policy.running_statements.len(), 1);
        assert_eq!(admitted.len(), 1);

        // Complete the second statement in the first transaction.
        let statement = admitted.pop_front().unwrap();
        admitted.extend(policy.complete_statement(statement));

        assert!(policy.running_statements.is_empty());
        assert_eq!(policy.completed_statements[&0].len(), 2);
        assert!(admitted.is_empty());

        // Enqueue the second statement in the second transaction.
        admitted.extend(policy.enqueue_statement(Statement::new(
            3,
            1,
            test_util::generate_plan("UPDATE T SET a = 1"),
        )));

        assert_eq!(policy.sidetracked_statements.len(), 1);

        // Enqueue the commit transaction statement in the first transaction.
        admitted.extend(policy.enqueue_statement(Statement::new(4, 0, Plan::CommitTransaction)));

        assert_eq!(policy.running_statements.len(), 1);
        assert_eq!(admitted.len(), 1);

        // Complete the commit transaction statement in the first transaction.
        let commit = admitted.pop_front().unwrap();
        admitted.extend(policy.complete_statement(commit));

        assert_eq!(policy.running_statements.len(), 1);
        assert!(!policy.completed_statements.contains_key(&0));
        assert!(policy.sidetracked_statements.is_empty());
        assert_eq!(admitted.len(), 1);

        // Complete the second statement in the second transaction.
        let statement = admitted.pop_front().unwrap();
        admitted.extend(policy.complete_statement(statement));

        assert!(policy.running_statements.is_empty());
        assert!(admitted.is_empty());

        // Enqueue the commit transaction statement in the second transaction.
        admitted.extend(policy.enqueue_statement(Statement::new(5, 1, Plan::CommitTransaction)));

        assert_eq!(policy.running_statements.len(), 1);
        assert_eq!(admitted.len(), 1);

        // Complete the commit transaction statement in the second transaction.
        let commit = admitted.pop_front().unwrap();
        admitted.extend(policy.complete_statement(commit));

        assert!(policy.running_statements.is_empty());
        assert!(policy.completed_statements.is_empty());
        assert!(policy.sidetracked_statements.is_empty());
        assert!(admitted.is_empty());
    }
}

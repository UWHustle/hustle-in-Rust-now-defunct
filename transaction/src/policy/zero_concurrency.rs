use hustle_common::plan::Plan;

use crate::{policy::Policy, Statement};
use crate::policy::PolicyHelper;

pub struct ZeroConcurrencyPolicy {
    policy_helper: PolicyHelper,
    running_statement: bool,

}

impl ZeroConcurrencyPolicy {
    pub fn new() -> Self {
        ZeroConcurrencyPolicy {
            policy_helper: PolicyHelper::new(),
            running_statement: false,
        }
    }

    fn safe_to_admit(&mut self, statement: &Statement) -> bool {
        !self.running_statement
            && self.policy_helper.sidetracked.front().unwrap().id == statement.transaction_id
    }

    fn admit_sidetracked(&mut self) -> Vec<(Plan, u64)> {
        let sidetracked = self.policy_helper.sidetracked_mut();
        while sidetracked.front()
            .map(|t| t.committed && t.statements.is_empty())
            .unwrap_or(false)
        {
            sidetracked.pop_front();
        }

        if !self.running_statement {
            if let Some(statement) = sidetracked.front_mut()
                .and_then(|t| t.statements.pop_front())
            {
                self.running_statement = true;
                vec![(statement.plan, statement.id)]
            } else {
                vec![]
            }
        } else {
            vec![]
        }
    }
}

impl Policy for ZeroConcurrencyPolicy {
    fn begin_transaction(&mut self) -> u64 {
        self.policy_helper.new_transaction()
    }

    fn commit_transaction(&mut self, transaction_id: u64) -> Vec<(Plan, u64)> {
        let transaction = self.policy_helper.get_transaction_mut(transaction_id);
        transaction.committed = true;
        if transaction.statements.is_empty() {
            self.admit_sidetracked()
        } else {
            vec![]
        }
    }

    fn enqueue_statement(&mut self, transaction_id: u64, plan: Plan) -> Vec<(Plan, u64)> {
        let statement = self.policy_helper.new_statement(transaction_id, plan);
        if self.safe_to_admit(&statement) {
            // TODO: Use a reference instead. Cloning the plan is inefficient.
            let plan = statement.plan.clone();
            self.running_statement = true;
            vec![(plan, statement.id)]
        } else {
            self.policy_helper.enqueue_statement(statement);
            vec![]
        }
    }

    fn complete_statement(&mut self, _statement_id: u64) -> Vec<(Plan, u64)> {
        self.running_statement = false;
        self.admit_sidetracked()
    }
}

#[cfg(test)]
mod zero_concurrency_policy_tests {
    use std::collections::VecDeque;

    use hustle_catalog::Table;
    use hustle_common::plan::{Plan, Query};
    use hustle_common::plan::QueryOperator::TableReference;

    use super::*;

    #[test]
    fn single_connection() {
        // Initialize the policy.
        let mut policy = ZeroConcurrencyPolicy::new();

        assert!(!policy.running_statement);
        assert!(policy.policy_helper.sidetracked.is_empty());

        // Begin a transaction.
        let transaction_id = policy.begin_transaction();

        assert_eq!(policy.policy_helper.sidetracked.len(), 1);

        let mut admitted = VecDeque::new();

        let plan = Plan::Query(Query {
            operator: TableReference(Table::new("T".to_owned(), vec![])),
            output: vec![],
        });

        // Enqueue the first statement in the transaction.
        collect_admitted(policy.enqueue_statement(transaction_id, plan.clone()), &mut admitted);

        assert!(policy.running_statement);
        assert_eq!(admitted.front(), Some(&0));

        // Enqueue the second statement in the transaction.
        collect_admitted(policy.enqueue_statement(transaction_id, plan), &mut admitted);

        assert_eq!(admitted.len(), 1);

        // Complete the first statement.
        let statement_id = admitted.pop_front().unwrap();
        collect_admitted(policy.complete_statement(statement_id), &mut admitted);

        assert_eq!(admitted.front(), Some(&1));

        // Complete the second statement.
        let statement_id = admitted.pop_front().unwrap();
        collect_admitted(policy.complete_statement(statement_id), &mut admitted);

        assert!(!policy.running_statement);
        assert!(admitted.is_empty());

        // Commit the transaction.
        collect_admitted(policy.commit_transaction(transaction_id), &mut admitted);

        assert!(policy.policy_helper.sidetracked.is_empty());
        assert!(admitted.is_empty());
    }

    #[test]
    fn multiple_connection() {
        // Initialize the policy.
        let mut policy = ZeroConcurrencyPolicy::new();

        assert!(!policy.running_statement);
        assert!(policy.policy_helper.sidetracked.is_empty());

        // Begin the first transaction.
        let first_transaction_id = policy.begin_transaction();

        assert_eq!(policy.policy_helper.sidetracked.len(), 1);

        // Begin the second transaction.
        let second_transaction_id = policy.begin_transaction();

        assert_eq!(policy.policy_helper.sidetracked.len(), 2);

        let mut admitted = VecDeque::new();

        let plan = Plan::Query(Query {
            operator: TableReference(Table::new("T".to_owned(), vec![])),
            output: vec![],
        });

        // Enqueue the first statement in the first transaction.
        collect_admitted(policy.enqueue_statement(first_transaction_id, plan.clone()), &mut admitted);

        assert!(policy.running_statement);
        assert_eq!(admitted.front(), Some(&0));

        // Enqueue the second statement in the second transaction.
        collect_admitted(policy.enqueue_statement(second_transaction_id, plan.clone()), &mut admitted);

        assert_eq!(admitted.len(), 1);

        // Enqueue the third statement in the first transaction.
        collect_admitted(policy.enqueue_statement(first_transaction_id, plan), &mut admitted);

        assert_eq!(admitted.len(), 1);

        // Complete the first statement.
        let statement_id = admitted.pop_front().unwrap();
        collect_admitted(policy.complete_statement(statement_id), &mut admitted);

        assert_eq!(admitted.front(), Some(&2));

        // Complete the third statement.
        let statement_id = admitted.pop_front().unwrap();
        collect_admitted(policy.complete_statement(statement_id), &mut admitted);

        assert!(admitted.is_empty());

        // Commit the first transaction.
        collect_admitted(policy.commit_transaction(first_transaction_id), &mut admitted);

        assert_eq!(policy.policy_helper.sidetracked.len(), 1);
        assert_eq!(admitted.front(), Some(&1));

        // Complete the second statement.
        let statement_id = admitted.pop_front().unwrap();
        collect_admitted(policy.complete_statement(statement_id), &mut admitted);

        assert!(!policy.running_statement);
        assert!(admitted.is_empty());

        collect_admitted(policy.commit_transaction(second_transaction_id), &mut admitted);

        assert!(policy.policy_helper.sidetracked.is_empty());
        assert!(admitted.is_empty());
    }

    fn collect_admitted(statements: Vec<(Plan, u64)>, admitted: &mut VecDeque<u64>) {
        for (_, statement_id) in statements {
            admitted.push_back(statement_id);
        }
    }
}

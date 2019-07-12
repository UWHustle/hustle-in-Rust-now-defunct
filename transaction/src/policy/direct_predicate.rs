use std::collections::{HashMap, HashSet};

use hustle_common::Plan;

use crate::{policy::Policy, Statement};
use crate::policy::PolicyHelper;

pub struct DirectPredicatePolicy {
    policy_helper: PolicyHelper,
    running: HashSet<Statement>,
    completed: HashMap<u64, Vec<Statement>>,
}

impl DirectPredicatePolicy {
    pub fn new() -> Self {
        DirectPredicatePolicy {
            policy_helper: PolicyHelper::new(),
            running: HashSet::new(),
            completed: HashMap::new(),
        }
    }

    fn conflicts_with_prior_statements_in_transaction(&self, statement: &Statement) -> bool {
        let transaction = self.policy_helper.get_transaction(statement.transaction_id);
        transaction.statements.iter()
            .take_while(|prior_statement| prior_statement.id != statement.id)
            .any(|prior_statement| statement.conflicts(prior_statement))
    }

    fn conflicts_with_running_statements(&self, statement: &Statement) -> bool {
        self.running.iter().any(|running_statement| statement.conflicts(running_statement))
    }

    fn conflicts_with_completed_statements(&self, statement: &Statement) -> bool {
        self.completed.iter().any(|(completed_transaction_id, completed_statements)|
            completed_transaction_id != &statement.transaction_id
                && completed_statements.iter().any(|completed_statement|
                    completed_statement.conflicts(statement)))
    }

    fn safe_to_admit(&self, statement: &Statement) -> bool {
        !self.conflicts_with_prior_statements_in_transaction(statement)
            && !self.conflicts_with_running_statements(statement)
            && !self.conflicts_with_completed_statements(statement)
    }

    fn admit_sidetracked(&mut self, callback: &Fn(Plan, u64)) {
        // TODO: Convert this logic to use drain_filter when it becomes stable.
        let mut transaction_i = 0;
        while transaction_i != self.policy_helper.sidetracked().len() {

            let mut statement_i = 0;
            while statement_i != self.policy_helper.sidetracked()[transaction_i].statements.len() {
                let statement = &self.policy_helper
                    .sidetracked()[transaction_i].statements[statement_i];

                if self.safe_to_admit(statement) {
                    let statement = self.policy_helper
                        .sidetracked_mut()[transaction_i].statements
                        .remove(statement_i).unwrap();
                    // TODO: Use a reference instead. Cloning the plan is inefficient.
                    let plan = statement.plan.clone();
                    let statement_id = statement.id;
                    self.running.insert(statement);
                    callback(plan, statement_id);
                } else {
                    statement_i += 1;
                }
            }

            let transaction = &self.policy_helper.sidetracked()[transaction_i];
            if transaction.statements.is_empty() && transaction.committed {
                let transaction = self.policy_helper.sidetracked.remove(transaction_i).unwrap();
                self.completed.remove(&transaction.id);
                transaction_i = 0;
            } else {
                transaction_i += 1;
            }
        }
    }
}

impl Policy for DirectPredicatePolicy {
    fn begin_transaction(&mut self) -> u64 {
        let transaction_id = self.policy_helper.new_transaction();
        self.completed.insert(transaction_id, vec![]);
        transaction_id
    }

    fn commit_transaction(&mut self, transaction_id: u64, callback: &Fn(Plan, u64)) {
        let transaction = self.policy_helper.get_transaction_mut(transaction_id);
        transaction.committed = true;
        if transaction.statements.is_empty() {
            self.completed.remove(&transaction_id);
            self.admit_sidetracked(callback);
        }
    }

    fn enqueue_statement(&mut self, transaction_id: u64, plan: Plan, callback: &Fn(Plan, u64)) {
        let statement = self.policy_helper.new_statement(transaction_id, plan);

        if self.safe_to_admit(&statement) {
            // TODO: Use a reference instead. Cloning the plan is inefficient.
            let plan = statement.plan.clone();
            let statement_id = statement.id;
            self.running.insert(statement);
            callback(plan, statement_id);
        } else {
            self.policy_helper.enqueue_statement(statement);
        }
    }

    fn complete_statement(&mut self, statement_id: u64, callback: &Fn(Plan, u64)) {
        let statement = self.running.take(&statement_id).unwrap();
        self.completed.get_mut(&statement.transaction_id).unwrap().push(statement);
        self.admit_sidetracked(callback);
    }
}

#[cfg(test)]
mod direct_predicate_policy_tests {
    use std::cell::RefCell;
    use std::collections::VecDeque;

    use crate::policy::{DirectPredicatePolicy, Policy};

    #[test]
    fn single_connection() {
        // Initialize the policy.
        let mut policy = DirectPredicatePolicy::new();

        assert!(policy.running.is_empty());
        assert!(policy.completed.is_empty());
        assert!(policy.policy_helper.sidetracked.is_empty());

        // Begin a transaction.
        let transaction_id = policy.begin_transaction();

        assert_eq!(policy.completed.len(), 1);
        assert!(policy.completed[&transaction_id].is_empty());
        assert_eq!(policy.policy_helper.sidetracked.len(), 1);

        // Enqueue the first statement in the transaction.
        let admitted = RefCell::new(VecDeque::new());
        let callback = |_, statement_id| admitted.borrow_mut().push_back(statement_id);
        let plan = util::generate_plan("SELECT a FROM T WHERE b = 1;");
        policy.enqueue_statement(transaction_id, plan, &callback);

        assert_eq!(policy.running.len(), 1);
        assert_eq!(admitted.borrow().len(), 1);

        // Enqueue the second statement in the transaction.
        let plan = util::generate_plan("UPDATE T SET c = 1 WHERE b = 2;");
        policy.enqueue_statement(transaction_id, plan, &callback);

        assert_eq!(policy.running.len(), 2);
        assert_eq!(admitted.borrow().len(), 2);

        // Enqueue the third statement in the transaction.
        let plan = util::generate_plan("INSERT INTO T VALUES (1, 2, 3);");
        policy.enqueue_statement(transaction_id, plan, &callback);

        assert_eq!(policy.policy_helper.sidetracked.front().unwrap().statements.len(), 1);

        // Complete the first statement.
        let statement_id = admitted.borrow_mut().pop_front().unwrap();
        policy.complete_statement(statement_id, &callback);

        assert_eq!(policy.running.len(), 1);
        assert_eq!(policy.completed[&transaction_id].len(), 1);

        // Complete the second statement.
        let statement_id = admitted.borrow_mut().pop_front().unwrap();
        policy.complete_statement(statement_id, &callback);

        assert_eq!(policy.running.len(), 1);
        assert_eq!(policy.completed[&transaction_id].len(), 2);
        assert!(policy.policy_helper.sidetracked.front().unwrap().statements.is_empty());
        assert_eq!(admitted.borrow().len(), 1);

        // Complete the third statement.
        let statement_id = admitted.borrow_mut().pop_front().unwrap();
        policy.complete_statement(statement_id, &callback);

        assert!(policy.running.is_empty());
        assert_eq!(policy.completed[&transaction_id].len(), 3);
        assert!(admitted.borrow().is_empty());

        // Commit the transaction.
        policy.commit_transaction(transaction_id, &callback);

        assert!(policy.completed.is_empty());
        assert!(policy.policy_helper.sidetracked.is_empty());
        assert!(admitted.borrow().is_empty());
    }

    #[test]
    fn multiple_connection() {
        // Initialize the policy.
        let mut policy = DirectPredicatePolicy::new();

        assert!(policy.running.is_empty());
        assert!(policy.completed.is_empty());
        assert!(policy.policy_helper.sidetracked.is_empty());

        // Begin the first transaction.
        let first_transaction_id = policy.begin_transaction();

        assert_eq!(policy.completed.len(), 1);
        assert!(policy.completed[&first_transaction_id].is_empty());
        assert_eq!(policy.policy_helper.sidetracked.len(), 1);

        // Begin the second transaction.
        let second_transaction_id = policy.begin_transaction();

        assert_eq!(policy.completed.len(), 2);
        assert!(policy.completed[&second_transaction_id].is_empty());
        assert_eq!(policy.policy_helper.sidetracked.len(), 2);

        // Enqueue the first statement in the first transaction.
        let admitted = RefCell::new(VecDeque::new());
        let callback = |_, statement_id| admitted.borrow_mut().push_back(statement_id);
        let plan = util::generate_plan("SELECT a FROM T WHERE b = 1;");
        policy.enqueue_statement(first_transaction_id, plan, &callback);

        assert_eq!(policy.running.len(), 1);
        assert_eq!(admitted.borrow().len(), 1);

        // Enqueue the second statement in the second transaction.
        let plan = util::generate_plan("UPDATE T SET a = 1;");
        policy.enqueue_statement(second_transaction_id, plan.clone(), &callback);

        assert_eq!(policy.policy_helper.get_transaction(second_transaction_id).statements.len(), 1);

        // Complete the first statement.
        let statement_id = admitted.borrow_mut().pop_front().unwrap();
        policy.complete_statement(statement_id, &callback);

        assert!(policy.running.is_empty());
        assert_eq!(policy.completed[&first_transaction_id].len(), 1);

        // Enqueue the third statement in the first transaction.
        policy.enqueue_statement(first_transaction_id, plan, &callback);

        assert_eq!(policy.running.len(), 1);
        assert_eq!(admitted.borrow().len(), 1);

        // Complete the third statement.
        let statement_id = admitted.borrow_mut().pop_front().unwrap();
        policy.complete_statement(statement_id, &callback);

        assert!(policy.running.is_empty());
        assert_eq!(policy.completed[&first_transaction_id].len(), 2);

        // Commit the first transaction.
        policy.commit_transaction(first_transaction_id, &callback);

        assert_eq!(policy.running.len(), 1);
        assert_eq!(admitted.borrow().len(), 1);
        assert_eq!(policy.completed.len(), 1);
        assert_eq!(policy.policy_helper.sidetracked.len(), 1);

        // Complete the second statement.
        let statement_id = admitted.borrow_mut().pop_front().unwrap();
        policy.complete_statement(statement_id, &callback);

        assert!(policy.running.is_empty());
        assert_eq!(policy.completed[&second_transaction_id].len(), 1);

        // Commit the second transaction.
        policy.commit_transaction(second_transaction_id, &callback);

        assert!(policy.completed.is_empty());
        assert!(policy.policy_helper.sidetracked.is_empty());
        assert!(admitted.borrow().is_empty());
    }
}

use std::collections::{HashMap, HashSet, VecDeque};

use message::Plan;

use crate::{policy::Policy, Statement};
use crate::transaction::Transaction;

pub struct DirectPredicatePolicy {
    sidetracked: VecDeque<Transaction>,
    running: HashSet<Statement>,
    completed: HashMap<u64, Vec<Statement>>,
    transaction_ctr: u64,
    statement_ctr: u64,
}

impl DirectPredicatePolicy {
    pub fn new() -> Self {
        DirectPredicatePolicy {
            sidetracked: VecDeque::new(),
            running: HashSet::new(),
            completed: HashMap::new(),
            transaction_ctr: 0,
            statement_ctr: 0,
        }
    }

    fn find_transaction(&self, transaction_id: u64) -> &Transaction {
        // TODO: Index the transactions on transaction id.
        self.sidetracked.iter()
            .find(|t| t.id == transaction_id)
            .unwrap()
    }

    fn find_transaction_mut(&mut self, transaction_id: u64) -> &mut Transaction {
        // TODO: Index the transactions on transaction id.
        self.sidetracked.iter_mut()
            .find(|t| t.id == transaction_id)
            .unwrap()
    }

    fn conflicts_with_prior_statements_in_transaction(&self, statement: &Statement) -> bool {
        let transaction = self.find_transaction(statement.transaction_id);
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
                && completed_statements.iter().all(|completed_statement|
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
        while transaction_i != self.sidetracked.len() {

            let mut statement_i = 0;
            while statement_i != self.sidetracked[transaction_i].statements.len() {
                let statement = &self.sidetracked[transaction_i].statements[statement_i];

                if self.safe_to_admit(statement) {
                    let statement = self.sidetracked[transaction_i].statements
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

            let transaction = &self.sidetracked[transaction_i];
            if transaction.statements.is_empty() && transaction.committed {
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
        let transaction_id = self.transaction_ctr;
        self.transaction_ctr = self.transaction_ctr.wrapping_add(1);
        let transaction = Transaction::new(transaction_id);
        self.sidetracked.push_back(transaction);
        self.completed.insert(transaction_id, vec![]);
        transaction_id
    }

    fn commit_transaction(&mut self, transaction_id: u64, callback: &Fn(Plan, u64)) {
        let committed_transaction = self.find_transaction_mut(transaction_id);
        committed_transaction.committed = true;
        if committed_transaction.statements.is_empty() {
            // TODO: Remove from sidetrack
            self.completed.remove(&transaction_id);
            self.admit_sidetracked(callback);
        }
    }

    fn enqueue_statement(&mut self, transaction_id: u64, plan: Plan, callback: &Fn(Plan, u64)) {
        let statement_id = self.statement_ctr;
        self.statement_ctr = self.statement_ctr.wrapping_add(1);
        let statement = Statement::new(statement_id, transaction_id, plan);

        if self.safe_to_admit(&statement) {
            // TODO: Use a reference instead. Cloning the plan is inefficient.
            let plan = statement.plan.clone();
            self.running.insert(statement);
            callback(plan, statement_id);
        } else {
            self.find_transaction_mut(transaction_id).statements.push_back(statement);
        }
    }

    fn complete_statement(&mut self, statement_id: u64, callback: &Fn(Plan, u64)) {
        let statement = self.running.take(&statement_id).unwrap();
        self.completed.get_mut(&statement.transaction_id).unwrap().push(statement);
        self.admit_sidetracked(callback);
    }
}

use std::collections::{HashMap, VecDeque};

pub use direct_predicate::DirectPredicatePolicy;
use hustle_common::plan::Plan;
pub use zero_concurrency::ZeroConcurrencyPolicy;

use crate::statement::Statement;
use crate::transaction::Transaction;

mod zero_concurrency;
mod direct_predicate;

pub trait Policy {
    fn begin_transaction(&mut self) -> u64;
    fn commit_transaction(&mut self, transaction_id: u64) -> Vec<(Plan, u64)>;
    fn enqueue_statement(&mut self, transaction_id: u64, plan: Plan) -> Vec<(Plan, u64)>;
    fn complete_statement(&mut self, statement_id: u64) -> Vec<(Plan, u64)>;
}

pub struct PolicyHelper {
    sidetracked: VecDeque<Transaction>,
    column_manager: ColumnManager,
    transaction_ctr: u64,
    statement_ctr: u64,
}

impl PolicyHelper {
    pub fn new() -> Self {
        PolicyHelper {
            sidetracked: VecDeque::new(),
            column_manager: ColumnManager::new(),
            transaction_ctr: 0,
            statement_ctr: 0,
        }
    }

    pub fn new_transaction(&mut self) -> u64 {
        let transaction_id = self.transaction_ctr;
        self.transaction_ctr = self.transaction_ctr.wrapping_add(1);
        self.sidetracked.push_back(Transaction::new(transaction_id));
        transaction_id
    }

    pub fn new_statement(&mut self, transaction_id: u64, plan: Plan) -> Statement {
        let statement_id = self.statement_ctr;
        self.statement_ctr = self.statement_ctr.wrapping_add(1);
        Statement::new(statement_id, transaction_id, plan, &mut self.column_manager)
    }

    pub fn enqueue_statement(&mut self, statement: Statement) {
        self.get_transaction_mut(statement.transaction_id).statements.push_back(statement);
    }

    pub fn sidetracked(&self) -> &VecDeque<Transaction> {
        &self.sidetracked
    }

    pub fn sidetracked_mut(&mut self) -> &mut VecDeque<Transaction> {
        &mut self.sidetracked
    }

    pub fn get_transaction(&self, transaction_id: u64) -> &Transaction {
        // TODO: Index the transactions on transaction id.
        self.sidetracked.iter()
            .find(|t| t.id == transaction_id)
            .unwrap()
    }

    pub fn get_transaction_mut(&mut self, transaction_id: u64) -> &mut Transaction {
        // TODO: Index the transactions on transaction id.
        self.sidetracked.iter_mut()
            .find(|t| t.id == transaction_id)
            .unwrap()
    }
}

pub struct ColumnManager {
    column_ids: HashMap<String, HashMap<String, u64>>,
    column_ctr: u64,
}

impl ColumnManager {
    pub fn new() -> Self {
        ColumnManager {
            column_ids: HashMap::new(),
            column_ctr: 0,
        }
    }

    pub fn get_column_id(&mut self, table: &str, column: &str) -> u64 {
        if let Some(column_id) = self.column_ids.get(table)
            .and_then(|columns| columns.get(column))
        {
            column_id.to_owned()
        } else {
            let column_id = self.column_ctr;
            self.column_ctr = self.column_ctr.wrapping_add(1);
            if let Some(columns) = self.column_ids.get_mut(table) {
                columns.insert(column.to_owned(), column_id);
            } else {
                let mut columns = HashMap::new();
                columns.insert(column.to_owned(), column_id);
                self.column_ids.insert(table.to_owned(), columns);
            }
            column_id
        }
    }
}

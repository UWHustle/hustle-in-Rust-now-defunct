use std::collections::HashMap;

use hustle_common::plan::Statement;
pub use predicate_comparison::PredicateComparisonPolicy;
pub use zero_concurrency::ZeroConcurrencyPolicy;

mod predicate_comparison;
mod zero_concurrency;

pub trait Policy {
    fn enqueue_statement(&mut self, statement: Statement) -> Vec<Statement>;
    fn complete_statement(&mut self, statement: Statement) -> Vec<Statement>;
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

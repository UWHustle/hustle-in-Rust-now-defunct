use logical_entities::column::Column;
use logical_entities::predicates::Predicate;
use logical_entities::row::Row;
use types::operators::Comparator;
use types::*;

pub struct Comparison {
    column: Column,
    comparator: Comparator,
    value: Box<Value>,
}

impl Comparison {
    pub fn new(column: Column, comparator: Comparator, value: Box<Value>) -> Self {
        Self {
            column,
            comparator,
            value,
        }
    }
}

impl Predicate for Comparison {
    fn evaluate(&self, row: &Row) -> bool {
        let all_values = row.get_values();
        let all_columns = row.get_schema().get_columns();
        for i in 0..all_columns.len() {
            if all_columns[i] == self.column {
                return all_values[i].compare(&*self.value, self.comparator.clone());
            }
        }
        panic!("Predicate column {} not found", self.column.get_name());
    }
}

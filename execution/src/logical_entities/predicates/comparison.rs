use logical_entities::column::Column;
use logical_entities::predicates::Predicate;
use logical_entities::row::Row;
use type_system::operators::Comparator;
use type_system::*;

pub struct Comparison {
    value: Box<Value>,
    comparator: Comparator,
    column: Column,
}

impl Comparison {
    pub fn new(value: Box<Value>, comparator: Comparator, column: Column) -> Self {
        Self {
            value,
            comparator,
            column,
        }
    }
}

impl Predicate for Comparison {
    fn evaluate(&self, row: &Row) -> bool {
        let all_values = row.get_values();
        let all_columns = row.get_schema().get_columns();
        for i in 0..all_columns.len() {
            if all_columns[i] == self.column {
                return self.value.compare(&*all_values[i], self.comparator.clone());
            }
        }
        panic!("Predicate column {} not found", self.column.get_name());
    }
}

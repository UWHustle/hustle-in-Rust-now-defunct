use logical_entities::column::Column;
use logical_entities::predicates::Predicate;
use logical_entities::row::Row;
use types::operators::Comparator;
use types::*;

pub enum ComparisonOperand {
    Value(Box<Value>),
    Column(Column),
}

pub struct Comparison {
    comparator: Comparator,
    l_operand: Column,
    r_operand: ComparisonOperand,
}

impl Comparison {
    pub fn new(
        comparator: Comparator,
        l_operand: Column,
        r_operand: ComparisonOperand,
    ) -> Self {
        Self {
            comparator,
            l_operand,
            r_operand,
        }
    }
}

impl Comparison {
    fn evaluate_column_value(&self, value: &Box<Value>, row: &Row) -> bool {
        // TODO: Improve efficiency by using the index of the column to compare
        let all_values = row.get_values();
        let all_columns = row.get_schema().get_columns();
        let column_i = all_columns.iter().position(|c| c == &self.l_operand)
            .expect(&format!("Predicate column {} not found", self.l_operand.get_name()));
        all_values[column_i].compare(&**value, self.comparator.clone())
    }

    fn evaluate_column_column(&self, column: &Column, row: &Row) -> bool {
        // TODO: Improve efficiency by using the index of the columns to compare
        let all_values = row.get_values();
        let all_columns = row.get_schema().get_columns();
        let l_column_i = all_columns.iter().position(|c| c == &self.l_operand)
            .expect(&format!("Predicate column {} not found", self.l_operand.get_name()));
        let r_column_i = all_columns.iter().position(|c| c == column)
            .expect(&format!("Predicate column {} not found", column.get_name()));
        all_values[l_column_i].compare(&*all_values[r_column_i], self.comparator.clone())
    }
}

impl Predicate for Comparison {
    fn evaluate(&self, row: &Row) -> bool {
        match &self.r_operand {
            ComparisonOperand::Value(value) => self.evaluate_column_value(value, row),
            ComparisonOperand::Column(column) => self.evaluate_column_column(column, row)
        }
    }
}

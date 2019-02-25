use logical_entities::column::Column;
use logical_entities::row::Row;
use type_system::Value;

// TODO: What's the overhead of a closure? (possible that unneeded context is preserved)
pub struct Predicate<T>
    where T: Fn(&[&Value]) -> bool {
    function: T,
    columns: Vec<Column>,
}

impl<T> Predicate<T>
    where T: Fn(&[&Value]) -> bool {
    pub fn new(function: T, columns: Vec<Column>) -> Self {
        Self {
            function,
            columns,
        }
    }

    pub fn evaluate(&self, row: Row) -> bool {
        let all_values = row.get_values();
        let all_columns = row.get_schema().get_columns();
        let mut predicate_values: Vec<&Value> = vec!();
        let mut j = 0;
        for i in 0..all_columns.len() {
            if all_columns[i] == self.columns[j] {
                predicate_values.push(&*all_values[i]);
                j += 1;
            }
        }
        (self.function)(predicate_values)
    }
}
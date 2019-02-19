pub mod aggregate;
pub mod export_csv;
pub mod import_csv;
pub mod insert;
pub mod join;
pub mod limit;
pub mod print;
pub mod project;
pub mod select_sum;
pub mod table_reference;
pub mod test_relation;

use logical_entities::relation::Relation;

pub trait Operator {
    // Returns the information for what relation will be returned when execute is called.
    fn get_target_relation(&self) -> Relation;

    // Executes the operator and returns the relation containing the results.
    fn execute(&self) -> Relation;
}

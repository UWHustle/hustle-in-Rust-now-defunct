
pub mod import_csv;
pub mod select_sum;
pub mod insert;
pub mod join;
pub mod print;
pub mod export_csv;
pub mod random_relation;


use logical_entities::relation::Relation;

pub trait Operator {
    fn execute(&self) -> Relation;
}
pub mod comparison;
pub mod connective;
pub mod tautology;

use logical_entities::row::Row;

pub trait Predicate {
    fn evaluate(&self, row: &Row) -> bool;
}

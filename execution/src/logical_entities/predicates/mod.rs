pub mod compare;
pub mod conjunction;
pub mod tautology;

use type_system::type_id::*;
use type_system::*;
use logical_entities::row::Row;

pub trait Predicate {
    fn evaluate(&self, row: &Row) -> bool;
}
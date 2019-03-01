use logical_entities::predicates::Predicate;
use logical_entities::row::Row;

pub struct Tautology {}

impl Tautology {
    pub fn new() -> Self {
        Tautology {}
    }
}

impl Predicate for Tautology {
    fn evaluate(&self, _row: &Row) -> bool {
        true
    }
}

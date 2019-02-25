use logical_entities::predicates::Predicate;
use logical_entities::row::Row;

pub enum ConjunctionType {
    And,
    Or,
}

impl ConjunctionType {
    fn apply(&self, val_1: bool, val_2: bool) -> bool {
        match self {
            ConjunctionType::And => val_1 && val_2,
            ConjunctionType::Or => val_1 || val_2,
        }
    }
}

pub struct Conjunction {
    conjunction_type: ConjunctionType,
    predicate_1: Box<Predicate>,
    predicate_2: Box<Predicate>,
}

impl Conjunction {
    fn new(conjunction_type: ConjunctionType, predicate_1: Box<Predicate>, predicate_2: Box<Predicate>) -> Self {
        Self {
            conjunction_type,
            predicate_1,
            predicate_2,
        }
    }
}

impl Predicate for Conjunction {
    fn evaluate(&self, row: &Row) -> bool {
        self.conjunction_type.apply(self.predicate_1.evaluate(row), self.predicate_2.evaluate(row))
    }
}
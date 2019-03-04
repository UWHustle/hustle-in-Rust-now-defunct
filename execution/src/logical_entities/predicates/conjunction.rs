use logical_entities::predicates::Predicate;
use logical_entities::row::Row;

pub enum ConnectiveType {
    And,
    Or,
}

impl ConnectiveType {
    fn apply(&self, val_1: bool, val_2: bool) -> bool {
        match self {
            ConnectiveType::And => val_1 && val_2,
            ConnectiveType::Or => val_1 || val_2,
        }
    }
}

pub struct Connective {
    conjunction_type: ConnectiveType,
    predicate_1: Box<Predicate>,
    predicate_2: Box<Predicate>,
}

impl Connective {
    fn new(
        conjunction_type: ConnectiveType,
        predicate_1: Box<Predicate>,
        predicate_2: Box<Predicate>,
    ) -> Self {
        Self {
            conjunction_type,
            predicate_1,
            predicate_2,
        }
    }
}

impl Predicate for Connective {
    fn evaluate(&self, row: &Row) -> bool {
        self.conjunction_type.apply(
            self.predicate_1.evaluate(row),
            self.predicate_2.evaluate(row),
        )
    }
}

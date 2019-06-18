use logical_entities::predicates::Predicate;
use logical_entities::row::Row;

pub enum ConnectiveType {
    And,
    Or,
}

impl ConnectiveType {
    pub fn from_str(string: &str) -> Self {
        match string {
            "and" => ConnectiveType::And,
            "or" => ConnectiveType::Or,
            _ => panic!("Unknown connective {}", string),
        }
    }

    fn apply(&self, values: Vec<bool>) -> bool {
        match self {
            ConnectiveType::And => {
                for value in values {
                    if !value {
                        return false;
                    }
                }
                return true;
            }
            ConnectiveType::Or => {
                for value in values {
                    if value {
                        return true;
                    }
                }
                return false;
            }
        }
    }
}

pub struct Connective {
    connective_type: ConnectiveType,
    terms: Vec<Box<Predicate>>,
}

impl Connective {
    pub fn new(connective_type: ConnectiveType, terms: Vec<Box<Predicate>>) -> Self {
        Self {
            connective_type,
            terms,
        }
    }
}

impl Predicate for Connective {
    fn evaluate(&self, row: &Row) -> bool {
        let mut results: Vec<bool> = vec![];
        for term in &self.terms {
            results.push(term.evaluate(row));
        }
        self.connective_type.apply(results)
    }
}

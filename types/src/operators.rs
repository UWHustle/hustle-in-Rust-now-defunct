use std::ops::{Add, Div, Mul, Sub};

/// Defines essential comparison operators
/// LessEq and GreaterEq can be defined via a combination of Less/Greater and Equal
#[derive(Clone)]
pub enum Comparator {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

impl Comparator {
    pub fn from_str(string: &str) -> Result<Self, String> {
        match string.to_lowercase().as_str() {
            "=" | "equal" => Ok(Comparator::Eq),
            "<" | "less" => Ok(Comparator::Lt),
            "<=" | "lessorequal" => Ok(Comparator::Le),
            ">" | "greater" => Ok(Comparator::Gt),
            ">=" | "greaterorequal" => Ok(Comparator::Ge),
            _ => Err(format!("unknown comparison {}", string)),
        }
    }

    pub fn apply<T: PartialOrd>(&self, val_1: T, val_2: T) -> bool {
        match self {
            Comparator::Eq => val_1 == val_2,
            Comparator::Lt => val_1 < val_2,
            Comparator::Le => val_1 <= val_2,
            Comparator::Gt => val_1 > val_2,
            Comparator::Ge => val_1 >= val_2,
        }
    }
}

/* ============================================================================================== */

/// Defines essential arithmetic operators
pub enum Arithmetic {
    Add,
    Subtract,
    Multiply,
    Divide,
}

impl Arithmetic {
    pub fn apply<T>(&self, val_1: T, val_2: T) -> T
    where
        T: Add<Output = T> + Sub<Output = T> + Mul<Output = T> + Div<Output = T>,
    {
        match self {
            Arithmetic::Add => val_1 + val_2,
            Arithmetic::Subtract => val_1 - val_2,
            Arithmetic::Multiply => val_1 * val_2,
            Arithmetic::Divide => val_1 / val_2,
        }
    }
}

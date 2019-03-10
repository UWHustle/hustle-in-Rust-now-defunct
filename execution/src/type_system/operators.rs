use std::ops::{Add, Div, Mul, Sub};

/// Defines essential comparison operators
/// LessEq and GreaterEq can be defined via a combination of Less/Greater and Equal
#[derive(Clone)]
pub enum Comparator {
    Equal,
    Less,
    LessEq,
    Greater,
    GreaterEq,
}

impl Comparator {
    pub fn from_str(string: &str) -> Self {
        match string {
            "=" | "Equal" => Comparator::Equal,
            "<" | "Less" => Comparator::Less,
            "<=" | "LessOrEqual" => Comparator::LessEq,
            ">" | "Greater" => Comparator::Greater,
            ">=" | "GreaterOrEqual" => Comparator::GreaterEq,
            _ => panic!("Unknown comparison {}", string),
        }
    }

    pub fn apply<T: PartialOrd>(&self, val_1: T, val_2: T) -> bool {
        match self {
            Comparator::Equal => val_1 == val_2,
            Comparator::Less => val_1 < val_2,
            Comparator::LessEq => val_1 <= val_2,
            Comparator::Greater => val_1 > val_2,
            Comparator::GreaterEq => val_1 >= val_2,
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

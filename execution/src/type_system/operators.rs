use std::ops::{Add, Div};

#[derive(Clone)]
pub enum Comparator {
    Less,
    Equal,
    Greater,
}

impl Comparator {
    pub fn apply<T: PartialOrd>(&self, val1: T, val2: T) -> bool {
        match self {
            Comparator::Less => {
                val1 < val2
            }
            Comparator::Equal => {
                val1 == val2
            }
            Comparator::Greater => {
                val1 > val2
            }
        }
    }
}

// Useful in preventing arithmetic code duplication
pub enum Arithmetic {
    Add,
    Divide,
}

impl Arithmetic {
    pub fn apply<T: Add<Output=T> + Div<Output=T>>(&self, val1: T, val2: T) -> T {
        match self {
            Arithmetic::Add => {
                val1 + val2
            }
            Arithmetic::Divide => {
                val1 / val2
            }
        }
    }
}
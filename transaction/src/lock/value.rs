use types::Value;
use types::operators::Comparator;
use ComparisonType::*;

#[derive(PartialEq)]
pub enum AccessMode { Read, Write }

pub enum ComparisonType { Eq, Lt, Le, Gt, Ge, Any }

pub struct ValueLock {
    access_mode: AccessMode,
    comparison_type: ComparisonType,
    value: Box<Value>,
}

impl ValueLock {
    pub fn new(
        access_mode: AccessMode,
        comparison_type: ComparisonType,
        value: Box<Value>,
    ) -> Self {
        ValueLock {
            access_mode,
            comparison_type,
            value,
        }
    }

    fn conflicts(&self, other: &Self) -> bool {
        if self.access_mode == AccessMode::Read && other.access_mode == AccessMode::Read {
            // Both locks are readers.
            false
        } else {
            // At least one lock is a writer. Determine whether the domains of the locks intersect.
            match self.comparison_type {
                Eq => {
                    match other.comparison_type {
                        Eq => self.value.compare(&*other.value, Comparator::Eq),
                        Lt => self.value.compare(&*other.value, Comparator::Lt),
                        Le => self.value.compare(&*other.value, Comparator::Le),
                        Gt => self.value.compare(&*other.value, Comparator::Gt),
                        Ge => self.value.compare(&*other.value, Comparator::Ge),
                        Any => true,
                    }
                },
                Lt => {
                    match other.comparison_type {
                        Eq | Gt | Ge => self.value.compare(&*other.value, Comparator::Gt),
                        Lt | Le | Any => true,
                    }
                },
                Le => {
                    match other.comparison_type {
                        Eq | Ge => self.value.compare(&*other.value, Comparator::Ge),
                        Gt => self.value.compare(&*other.value, Comparator::Gt),
                        Lt | Le | Any => true,
                    }
                },
                Gt => {
                    match other.comparison_type {
                        Eq | Lt | Le => self.value.compare(&*other.value, Comparator::Lt),
                        Gt | Ge | Any => true,
                    }
                },
                Ge => {
                    match other.comparison_type {
                        Eq | Le => self.value.compare(&*other.value, Comparator::Le),
                        Lt => self.value.compare(&*other.value, Comparator::Lt),
                        Gt | Ge | Any => true,
                    }
                }
                Any => true,
            }
        }
    }
}

use crate::lock::{AccessMode, ValueLock};
use std::hash::{Hash, Hasher};

pub struct PredicateLock {
    statement_id: u64,
    access_mode: AccessMode,
    value_locks: Vec<ValueLock>,
}

impl PredicateLock {
    pub fn new(statement_id: u64, access_mode: AccessMode, value_locks: Vec<ValueLock>) -> Self {
        PredicateLock {
            statement_id,
            access_mode,
            value_locks,
        }
    }

    pub fn conflicts(&self, other: &Self) -> bool {
        unimplemented!()
    }
}

impl PartialEq for PredicateLock {
    fn eq(&self, other: &Self) -> bool {
        self.statement_id.eq(&other.statement_id)
    }
}

impl Eq for PredicateLock {}

impl Hash for PredicateLock {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.statement_id.hash(state);
    }
}

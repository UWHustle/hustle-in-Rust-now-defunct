use std::borrow::Borrow;
use std::hash::{Hash, Hasher};

use hustle_common::Plan;

use crate::lock::{AccessMode, PredicateLock};

pub struct Statement {
    pub id: u64,
    pub transaction_id: u64,
    pub plan: Plan,
    pub predicate_lock: PredicateLock,
}

impl Statement {
    pub fn new(id: u64, transaction_id: u64, plan: Plan) -> Self {
        let predicate_lock = Self::generate_lock(&plan);
        Statement {
            id,
            transaction_id,
            plan,
            predicate_lock,
        }
    }

    pub fn conflicts(&self, other: &Self) -> bool {
        self.predicate_lock.conflicts(&other.predicate_lock)
    }

    fn generate_lock(_plan: &Plan) -> PredicateLock {
        // TODO: Implement this logic.
        PredicateLock::new(AccessMode::Read, vec![])
    }
}

impl PartialEq for Statement {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl Eq for Statement {}

impl Hash for Statement {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Borrow<u64> for Statement {
    fn borrow(&self) -> &u64 {
        &self.id
    }
}

use crate::lock::{AccessMode, ValueLock};

pub struct PredicateLock {
    _access_mode: AccessMode,
    _value_locks: Vec<ValueLock>,
}

impl PredicateLock {
    pub fn new(access_mode: AccessMode, value_locks: Vec<ValueLock>) -> Self {
        PredicateLock {
            _access_mode: access_mode,
            _value_locks: value_locks,
        }
    }

    pub fn conflicts(&self, _other: &Self) -> bool {
        unimplemented!()
    }
}

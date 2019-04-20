use std::collections::HashSet;
use std::sync::{Condvar, Mutex};

pub trait RecordGuard {
    fn lock(&self, key: &str);
    fn unlock(&self, key: &str);
}

pub struct MutexRecordGuard {
    locked: (Mutex<HashSet<String>>, Condvar)
}

impl MutexRecordGuard {
    pub fn new() -> Self {
        MutexRecordGuard {
            locked: (Mutex::new(HashSet::new()), Condvar::new())
        }
    }
}

impl RecordGuard for MutexRecordGuard {
    fn lock(&self, key: &str) {
        let &(ref locked, ref cvar) = &self.locked;
        let mut locked_guard = locked.lock().unwrap();
        while locked_guard.contains(key) {
            locked_guard = cvar.wait(locked_guard).unwrap();
        }
        locked_guard.insert(key.to_string());
    }

    fn unlock(&self, key: &str) {
        let &(ref locked, ref cvar) = &self.locked;
        let mut locked_guard = locked.lock().unwrap();
        locked_guard.remove(key);
        cvar.notify_all();
    }
}

#[allow(unused)]
pub struct NoLockRecordGuard {}

impl RecordGuard for NoLockRecordGuard {
    fn lock(&self, _key: &str) {}

    fn unlock(&self, _key: &str) {}
}
use std::collections::HashSet;
use std::sync::{Condvar, Mutex};

pub trait RecordGuard {
    fn begin_read(&self, key: &str);
    fn begin_write(&self, key: &str);
    fn end_read(&self, key: &str);
    fn end_write(&self, key: &str);
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
    fn begin_read(&self, key: &str) {
        let &(ref locked, ref cvar) = &self.locked;
        let mut locked_guard = locked.lock().unwrap();
        while locked_guard.contains(key) {
            locked_guard = cvar.wait(locked_guard).unwrap();
        }
        locked_guard.insert(key.to_string());
    }

    fn begin_write(&self, key: &str) {
        self.begin_read(key);
    }

    fn end_read(&self, key: &str) {
        let &(ref locked, ref cvar) = &self.locked;
        let mut locked_guard = locked.lock().unwrap();
        locked_guard.remove(key);
        cvar.notify_all();
    }

    fn end_write(&self, key: &str) {
        self.end_read(key)
    }
}

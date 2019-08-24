use std::sync::{RwLock, RwLockReadGuard, Mutex, RwLockWriteGuard, MutexGuard};

pub enum LatchGuard<'a> {
    MutexGuard(MutexGuard<'a, ()>),
    ReadGuard(RwLockReadGuard<'a, ()>),
    WriteGuard(RwLockWriteGuard<'a, ()>),
    NoneGuard,
}

pub enum Latch {
    Mutex(Mutex<()>),
    RwLatch(RwLock<()>),
    None,
}

impl Latch {
    pub fn mutex() -> Self {
        Latch::Mutex(Mutex::new(()))
    }

    pub fn rw_latch() -> Self {
        Latch::RwLatch(RwLock::new(()))
    }

    pub fn none() -> Self {
        Latch::None
    }

    pub fn read(&self) -> LatchGuard {
        match self {
            Latch::RwLatch(lock) => LatchGuard::ReadGuard(lock.read().unwrap()),
            Latch::Mutex(lock) => LatchGuard::MutexGuard(lock.lock().unwrap()),
            Latch::None => LatchGuard::NoneGuard,
        }
    }

    pub fn write(&self) -> LatchGuard {
        match self {
            Latch::RwLatch(lock) => LatchGuard::WriteGuard(lock.write().unwrap()),
            Latch::Mutex(lock) => LatchGuard::MutexGuard(lock.lock().unwrap()),
            Latch::None => LatchGuard::NoneGuard,
        }
    }
}

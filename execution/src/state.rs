use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

use owning_ref::OwningHandle;

pub type Tentative = Mutex<HashMap<u64, Arc<Mutex<Vec<usize>>>>>;
pub type Inserted = Mutex<HashMap<u64, Arc<Mutex<HashSet<usize>>>>>;
pub type BlockTentativeGuard<'a> = OwningHandle<Arc<Mutex<Vec<usize>>>, MutexGuard<'a, Vec<usize>>>;
pub type BlockInsertedGuard<'a> = OwningHandle<Arc<Mutex<HashSet<usize>>>, MutexGuard<'a, HashSet<usize>>>;

pub struct TransactionState {
    pub id: u64,
    tentative: Tentative,
    inserted: Inserted,
}

impl TransactionState {
    pub fn new(id: u64) -> Self {
        TransactionState {
            id,
            tentative: Mutex::new(HashMap::new()),
            inserted: Mutex::new(HashMap::new()),
        }
    }

    pub fn get_tentative(&self) -> &Tentative {
        &self.tentative
    }

    pub fn lock_tentative_for_block(&self, block_id: u64) -> BlockTentativeGuard {
        OwningHandle::new_with_fn(
            self.tentative.lock().unwrap().entry(block_id).or_default().clone(),
            |t| unsafe { (*t).lock().unwrap() },
        )
    }

    pub fn lock_inserted_for_block(&self, block_id: u64) -> BlockInsertedGuard {
        OwningHandle::new_with_fn(
            self.inserted.lock().unwrap().entry(block_id).or_default().clone(),
            |t| unsafe { (*t).lock().unwrap() },
        )
    }
}

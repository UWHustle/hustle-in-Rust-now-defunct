use std::sync::{Arc, Mutex, Condvar};
use block::RowMajorBlock;
use std::ops::Deref;

pub struct BlockReference {
    pub id: u64,
    block: Arc<RowMajorBlock>,
    rc: Arc<(Mutex<u64>, Condvar)>,
}

impl BlockReference {
    pub fn new(id: u64, block: RowMajorBlock) -> Self {
        BlockReference {
            id,
            block: Arc::new(block),
            rc: Arc::new((Mutex::new(1), Condvar::new())),
        }
    }

    pub fn get_reference_count(&self) -> &(Mutex<u64>, Condvar) {
        &self.rc
    }
}

impl Deref for BlockReference {
    type Target = RowMajorBlock;

    fn deref(&self) -> &Self::Target {
        &self.block
    }
}

impl Clone for BlockReference {
    fn clone(&self) -> Self {
        let mut rc_guard = self.rc.0.lock().unwrap();
        *rc_guard += 1;
        BlockReference {
            id: self.id,
            block: self.block.clone(),
            rc: self.rc.clone(),
        }
    }
}

impl Drop for BlockReference {
    fn drop(&mut self) {
        // Decrement the reference count when dropped.
        let &(ref rc_lock, ref cvar) = &*self.rc;
        let mut rc_guard = rc_lock.lock().unwrap();
        *rc_guard -= 1;
        cvar.notify_all();
    }
}

const DEFAULT_LOG_DIRECTORY: &str = "data";

enum TransactionState {
    Begun = 0,
    Committed = 1,
    Completed = 2,
}

pub struct LogManager {
    dir: String,
}

impl LogManager {
    pub fn default() -> Self {
        LogManager {
            dir: DEFAULT_LOG_DIRECTORY.to_owned(),
        }
    }

    pub fn log_begin_transaction(&self, id: u64) {
        unimplemented!()
    }

    pub fn log_commit_transaction(&self, id: u64) {
        unimplemented!()
    }

    pub fn log_complete_transaction(&self, id: u64) {
        unimplemented!()
    }

    pub fn log_insert(&self, transaction_id: u64, block_id: u64, row_id: u64) {
        unimplemented!()
    }

    pub fn log_delete(&self, transaction_id: u64, block_id: u64, row_id: u64) {
        unimplemented!()
    }

    pub fn log_update(
        &self,
        transaction_id: u64,
        block_id: u64,
        row_id: u64,
        col_id: u64,
        buf: &[u8]
    ) {
        unimplemented!()
    }

    pub fn flush(&self) {
        unimplemented!()
    }
}

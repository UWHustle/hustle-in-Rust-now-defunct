use std::fs;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use byteorder::{LittleEndian, WriteBytesExt};
use uuid::Uuid;

const DEFAULT_LOG_DIRECTORY: &str = "logs";
const TRANSACTION_LOG_FILE_NAME: &str = "transaction";
const INSERT_LOG_FILE_NAME: &str = "insert";
const DELETE_LOG_FILE_NAME: &str = "delete";
const UPDATE_LOG_FILE_NAME: &str = "update";

#[derive(Clone, Copy)]
enum TransactionState {
    Begun = 0,
    Committed = 1,
    Completed = 2,
}

struct TransactionLogEntry {
    transaction_id: u64,
    transaction_state: TransactionState,
}

impl TransactionLogEntry {
    fn to_buf(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.write_u64::<LittleEndian>(self.transaction_id).unwrap();
        buf.write_u8(self.transaction_state as u8).unwrap();
        buf
    }
}

struct InsertDeleteLogEntry {
    transaction_id: u64,
    block_id: u64,
    row_id: u64,
}

impl InsertDeleteLogEntry {
    fn to_buf(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.write_u64::<LittleEndian>(self.transaction_id).unwrap();
        buf.write_u64::<LittleEndian>(self.block_id).unwrap();
        buf.write_u64::<LittleEndian>(self.row_id).unwrap();
        buf
    }
}

struct UpdateLogEntry<D> {
    transaction_id: u64,
    block_id: u64,
    row_id: u64,
    col_id: u64,
    buf: D,
}

impl<D> UpdateLogEntry<D> where D: Deref<Target = [u8]> {
    fn to_buf(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.write_u64::<LittleEndian>(self.transaction_id).unwrap();
        buf.write_u64::<LittleEndian>(self.block_id).unwrap();
        buf.write_u64::<LittleEndian>(self.row_id).unwrap();
        buf.write_u64::<LittleEndian>(self.col_id).unwrap();
        buf.write(&self.buf).unwrap();
        buf
    }
}

pub struct LogManager {
    dir: String,
    transaction_log: Mutex<File>,
    insert_log: Mutex<File>,
    delete_log: Mutex<File>,
    update_log: Mutex<File>,
}

impl LogManager {
    pub fn default() -> Self {
        Self::with_log_directory(DEFAULT_LOG_DIRECTORY.to_owned())
    }

    pub fn with_unique_log_directory() -> Self {
        Self::with_log_directory(Uuid::new_v4().to_string())
    }

    fn with_log_directory(dir: String) -> Self {
        let path = Path::new(&dir);
        if !path.exists() {
            fs::create_dir(&dir).unwrap();
        }

        let transaction_log = Mutex::new(Self::file(&dir, TRANSACTION_LOG_FILE_NAME));
        let insert_log = Mutex::new(Self::file(&dir, INSERT_LOG_FILE_NAME));
        let delete_log = Mutex::new(Self::file(&dir, DELETE_LOG_FILE_NAME));
        let update_log = Mutex::new(Self::file(&dir, UPDATE_LOG_FILE_NAME));

        LogManager {
            dir,
            transaction_log,
            insert_log,
            delete_log,
            update_log,
        }
    }

    pub fn log_begin_transaction(&self, transaction_id: u64) {
        let entry = TransactionLogEntry {
            transaction_id,
            transaction_state: TransactionState::Begun,
        };

        self.transaction_log.lock().unwrap().write(&entry.to_buf()).unwrap();
    }

    pub fn log_commit_transaction(&self, transaction_id: u64) {
        let entry = TransactionLogEntry {
            transaction_id,
            transaction_state: TransactionState::Committed,
        };

        self.transaction_log.lock().unwrap().write(&entry.to_buf()).unwrap();
    }

    pub fn log_complete_transaction(&self, transaction_id: u64) {
        let entry = TransactionLogEntry {
            transaction_id,
            transaction_state: TransactionState::Completed,
        };

        self.transaction_log.lock().unwrap().write(&entry.to_buf()).unwrap();
    }

    pub fn log_insert(&self, transaction_id: u64, block_id: u64, row_id: u64) {
        let entry = InsertDeleteLogEntry {
            transaction_id,
            block_id,
            row_id,
        };

        self.insert_log.lock().unwrap().write(&entry.to_buf()).unwrap();
    }

    pub fn log_delete(&self, transaction_id: u64, block_id: u64, row_id: u64) {
        let entry = InsertDeleteLogEntry {
            transaction_id,
            block_id,
            row_id,
        };

        self.delete_log.lock().unwrap().write(&entry.to_buf()).unwrap();
    }

    pub fn log_update(
        &self,
        transaction_id: u64,
        block_id: u64,
        row_id: u64,
        col_id: u64,
        buf: &[u8]
    ) {
        let entry = UpdateLogEntry {
            transaction_id,
            block_id,
            row_id,
            col_id,
            buf,
        };

        self.update_log.lock().unwrap().write(&entry.to_buf()).unwrap();
    }

    fn file(dir: &str, file_name: &str) -> File {
        let mut path = PathBuf::from(dir);
        path.push(file_name);
        path.set_extension("log");

        OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .unwrap()
    }

    pub fn clear(&self) {
        fs::remove_dir_all(&self.dir).unwrap();
    }
}

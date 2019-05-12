extern crate memmap;

pub mod storage_manager;

mod key_value_storage_engine;
mod relational_storage_engine;
mod buffer_manager;
mod physical_relation;
mod relational_block;
mod record_guard;

pub use storage_manager::StorageManager;

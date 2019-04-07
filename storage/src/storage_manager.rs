extern crate memmap;
extern crate omap;

use std::sync::Mutex;
use std::mem;
use buffer::{Block, Buffer};
use record_guard::{MutexRecordGuard, RecordGuard};
use std::cmp::min;
use std::ops::Deref;

const DEFAULT_BUFFER_CAPACITY: usize = 1000;
const BLOCK_SIZE: usize = 1000;
const TEMP_RECORD_PREFIX: char = '$';

pub trait RawRecordAccess {
    fn get_byte(&self, byte: usize) -> Option<u8>;
}

pub trait BlockRecordAccess {
    fn blocks(&self) -> RecordBlockIter;
    fn get_block(&self, block_index: usize) -> Option<RecordBlock>;
}

pub trait StructuredRecordAccess<'a> {
    fn get_row_col(&'a mut self, row: usize, col: usize) -> Option<&[u8]>;
    fn get_row(&'a mut self, row: usize) -> RowIter;
    fn get_col(&'a mut self, col: usize) -> ColIter;
}

/// A record that represents a key-value pair in the `StorageManager`. The value can be accessed
/// with raw byte offset or at the block level for bulk operations.
pub struct Record<'a> {
    key: &'a str,
    buffer: &'a Buffer,
    record_guard: &'a Box<RecordGuard + Send + Sync>
}

impl<'a> Record<'a> {
    /// Creates a new `Record`.
    fn new(key: &'a str, buffer: &'a Buffer,
           record_guard: &'a Box<RecordGuard + Send + Sync>) -> Self {
        Record {
            key,
            buffer,
            record_guard
        }
    }
}

impl<'a> RawRecordAccess for Record<'a> {
    fn get_byte(&self, byte: usize) -> Option<u8> {
        unimplemented!()
    }
}

impl<'a> BlockRecordAccess for Record<'a> {
    /// Returns an iterator over the record's blocks.
    fn blocks(&self) -> RecordBlockIter {
        RecordBlockIter::new(self)
    }

    /// Returns a reference to the block at the specified `block_index` if it exists.
    fn get_block(&self, block_index: usize) -> Option<RecordBlock> {
        let key_for_block = StorageManager::key_for_block(self.key, block_index);
        self.buffer.get(&key_for_block).map(|block| (RecordBlock::new(self, block)))
    }
}

pub struct StructuredRecord<'a> {
    record: Record<'a>,
    schema: &'a [usize],
    row_size: usize,
    rows_per_block: usize,
    current_block: Option<Block>,
    current_block_index: usize
}

impl<'a> StructuredRecord<'a> {
    fn new(key: &'a str, buffer: &'a Buffer, record_guard: &'a Box<RecordGuard + Send + Sync>,
           schema: &'a [usize]) -> Self {
        // Pre-compute some frequently used constants.
        let row_size: usize = schema.iter().sum();
        let rows_per_block: usize = BLOCK_SIZE / row_size;

        StructuredRecord {
            record: Record::new(key, buffer, record_guard),
            schema,
            row_size,
            rows_per_block,
            current_block: None,
            current_block_index: 0
        }
    }
}

impl<'a> RawRecordAccess for StructuredRecord<'a> {
    fn get_byte(&self, byte: usize) -> Option<u8> {
        self.record.get_byte(byte)
    }
}

impl<'a> BlockRecordAccess for StructuredRecord<'a> {
    /// Returns an iterator over the record's blocks.
    fn blocks(&self) -> RecordBlockIter {
        self.record.blocks()
    }

    /// Returns a reference to the block at the specified `block_index` if it exists.
    fn get_block(&self, block_index: usize) -> Option<RecordBlock> {
        self.record.get_block(block_index)
    }
}

impl<'a> StructuredRecordAccess<'a> for StructuredRecord<'a> {
    fn get_row_col(&mut self, row: usize, col: usize) -> Option<&[u8]> {
        let block_index = row / self.rows_per_block;
        let row_in_block = row % self.rows_per_block;

        if self.current_block.is_none() || block_index != self.current_block_index {
            let key_for_block = StorageManager::key_for_block(self.record.key, block_index);
            self.current_block = self.record.buffer.get(&key_for_block);
        }

        self.current_block.as_ref()
            .map(|block| {
                let offset_in_row: usize = self.schema[..col].iter().sum();
                let offset_in_block =  self.row_size * row_in_block + offset_in_row;
                &block[offset_in_block..offset_in_block + self.schema[col]]
            })
    }

    fn get_row(&'a mut self, row: usize) -> RowIter {
        RowIter::new(self, row)
    }

    fn get_col(&'a mut self, col: usize) -> ColIter {
        ColIter::new(self, col)
    }
}

/// A block for a `Record`.
pub struct RecordBlock<'a> {
    #[allow(unused)]
    record: &'a Record<'a>,
    block: Block
}

impl<'a> RecordBlock<'a> {
    fn new(record: &'a Record, block: Block) -> Self {
        RecordBlock {
            record,
            block
        }
    }
}

impl<'a> Deref for RecordBlock<'a> {
    type Target = Block;

    fn deref(&self) -> &Block {
        &self.block
    }
}

/// An iterator over the blocks of a `Record`.
pub struct RecordBlockIter<'a> {
    record: &'a Record<'a>,
    block_index: usize
}

impl<'a> RecordBlockIter<'a> {
    fn new(record: &'a Record) -> Self {
        RecordBlockIter {
            record,
            block_index: 0
        }
    }
}

impl<'a> Iterator for RecordBlockIter<'a> {
    type Item = RecordBlock<'a>;

    fn next(&mut self) -> Option<RecordBlock<'a>> {
        let block = self.record.get_block(self.block_index);
        self.block_index += 1;
        block
    }
}

impl<'a> Drop for Record<'a> {
    fn drop(&mut self) {
        self.record_guard.end_read(self.key);
    }
}

pub struct RowIter<'a> {
    structured_record: &'a mut StructuredRecord<'a>,
    row: usize,
    col: usize
}

impl<'a> RowIter<'a> {
    fn new(structured_record: &'a mut StructuredRecord<'a>, row: usize) -> Self {
        RowIter {
            structured_record,
            row,
            col: 0
        }
    }

    fn next(&'a mut self) -> Option<&[u8]> {
        let value = self.structured_record.get_row_col(self.row, self.col);
        self.col += 1;
        value
    }
}

pub struct ColIter<'a> {
    structured_record: &'a mut StructuredRecord<'a>,
    row: usize,
    col: usize
}

impl<'a> ColIter<'a> {
    fn new(structured_record: &'a mut StructuredRecord<'a>, col: usize) -> Self {
        ColIter {
            structured_record,
            row: 0,
            col
        }
    }

    fn next(&'a mut self) -> Option<&'a [u8]> {
        let value = self.structured_record.get_row_col(self.row, self.col);
        self.col += 1;
        value
    }
}

/// A storage manager for persisting and caching key-value pairs. Keys are strings and values are
/// arbitrarily long arrays of bytes. The `StorageManager` breaks the values into uniform blocks and
/// writes them to the `Buffer`, which handles caching. When the value for a key is requested, a
/// `Record` is returned, which provides access to the value at the block level. The
/// `StorageManager` handles concurrency through a `RecordGuard` policy.
pub struct StorageManager {
    buffer: Buffer,
    record_guard: Box<RecordGuard + Send + Sync>,
    anon_ctr: Mutex<u64>
}

impl StorageManager {

    /// Creates a new `StorageManager`.
    pub fn new() -> Self {
        Self::with_buffer_capacity(DEFAULT_BUFFER_CAPACITY)
    }

    /// Creates a new `StorageManager` with the specified buffer capacity.
    pub fn with_buffer_capacity(buffer_capacity: usize) -> Self {
        StorageManager {
            buffer: Buffer::with_capacity(buffer_capacity),
            record_guard: Box::new(MutexRecordGuard::new()),
            anon_ctr: Mutex::new(0)
        }
    }

    /// Writes the key-value pair to the buffer. If the size of the value is greater than the block
    /// size, the value is broken down into blocks.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use storage::StorageManager;
    /// let sm = StorageManager::new();
    /// sm.put("key", b"value");
    /// ```
    pub fn put(&self, key: &str, value: &[u8]) {
        self.record_guard.begin_write(key);

        let block_count = (value.len()  - 1) / BLOCK_SIZE + 1;
        for block_index in 0..block_count {
            let key_for_block = Self::key_for_block(key, block_index);
            let value_offset = block_index * BLOCK_SIZE;
            let value_right_bound = value_offset + min(BLOCK_SIZE, value.len() - value_offset);
            let value_for_block = &value[value_offset..value_right_bound];
            self.buffer.write(&key_for_block, value_for_block);
        }

        self.record_guard.end_write(key);
    }

    /// Writes the value to the buffer, returning a unique key that can be used to request it.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use storage::StorageManager;
    /// let sm = StorageManager::new();
    /// let key = sm.put_anon(b"value");
    /// sm.get(&key);
    /// ```
    pub fn put_anon(&self, value: &[u8]) -> String {
        // Generate a unique key, prefixed with the reserved character.
        let mut anon_ctr = self.anon_ctr.lock().unwrap();
        let key = format!("{}{}", TEMP_RECORD_PREFIX, *anon_ctr);
        *anon_ctr = anon_ctr.wrapping_add(1);
        mem::drop(anon_ctr);

        // Put the value and return the key.
        self.put(key.as_str(), value);
        key
    }

    /// Gets the value associated with `key` from the buffer and returns a `Record` if it exists.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use storage::StorageManager;
    /// let sm = StorageManager::new();
    /// sm.put("key", b"value");
    /// sm.get("key");
    /// ```
//    pub fn get<'a>(&'a self, key: &'a str) -> Option<Record<'a>> {
//        self.record_guard.begin_read(key);
//        let key_for_block = Self::key_for_block(key, 0);
//        if self.buffer.exists(&key_for_block) {
//            Some(Record::new(key, &self.buffer, &self.record_guard))
//            // When the record drops, record_guard.end_read(key) is called
//        } else {
//            self.record_guard.end_read(key);
//            None
//        }
//    }

    pub fn get_with_schema<'a>(&'a self, key: &'a str,
                               schema: &'a [usize]) -> Option<StructuredRecord<'a>> {
        self.record_guard.begin_read(key);
        let key_for_block = Self::key_for_block(key, 0);
        if self.buffer.exists(&key_for_block) {
            Some(StructuredRecord::new(key, &self.buffer, &self.record_guard, schema))
        } else {
            self.record_guard.end_read(key);
            None
        }
    }

    /// Deletes the key-value pair.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use storage::StorageManager;
    /// let sm = StorageManager::new();
    /// sm.put("key", b"value");
    /// sm.delete("key");
    /// ```
    pub fn delete(&self, key: &str) {
        self.record_guard.begin_write(key);
        self.buffer.erase(key);
        self.record_guard.end_write(key);
    }

    fn key_for_block(key: &str, block_index: usize) -> String {
        format!("{}.{}", key, block_index)
    }
}

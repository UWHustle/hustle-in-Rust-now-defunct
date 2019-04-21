extern crate memmap;
extern crate omap;

use std::sync::Mutex;
use std::mem;
use buffer::{Block, Buffer, BLOCK_SIZE};
use record_guard::{MutexRecordGuard, RecordGuard};
use std::cmp::min;
use std::ops::Deref;
use std::io::BufReader;

const DEFAULT_BUFFER_CAPACITY: usize = 1000;
const TEMP_RECORD_PREFIX: char = '$';

pub struct Record<'a> {
    key: &'a str,
    buffer: &'a Buffer,
    record_guard: &'a Box<RecordGuard + Send + Sync>,
    column_offsets: Vec<usize>,
    row_size: usize,
    rows_per_block: usize
}

/// A record that represents a key-value pair in the `StorageManager`. The value can be accessed
/// with raw byte offset, at the block level for bulk operations, or at the row/column level for
/// structured data processing.
impl<'a> Record<'a> {
    /// Creates a new `Record`.
    fn new(key: &'a str,
           buffer: &'a Buffer,
           record_guard: &'a Box<RecordGuard + Send + Sync>) -> Self {
        Self::with_schema(key, buffer, record_guard, &[1])
    }

    /// Creates a new `Record` with the specified schema to be used for row/column access.
    fn with_schema(key: &'a str,
                   buffer: &'a Buffer,
                   record_guard: &'a Box<RecordGuard + Send + Sync>,
                   schema: &'a [usize]) -> Self {

        // Pre-compute some frequently used constants.
        let mut row_size = 0;
        let mut column_offsets = Vec::with_capacity(schema.len() + 1);
        column_offsets.push(row_size);
        for column_size in schema {
            row_size += column_size;
            column_offsets.push(row_size);
        }

        let rows_per_block: usize = BLOCK_SIZE / row_size;

        Record {
            key,
            buffer,
            record_guard,
            column_offsets,
            row_size,
            rows_per_block
        }
    }

    /// Returns an iterator over the record's blocks.
    pub fn blocks(&self) -> RecordBlockIter {
        RecordBlockIter::new(self)
    }

    /// Returns a reference to the block at the specified `block_index` if it exists.
    pub fn get_block(&self, block_index: usize) -> Option<RecordBlock> {
        let key_for_block = StorageManager::key_for_block(self.key, block_index);
        self.buffer.get(&key_for_block).map(|block|
            RecordBlock::new(
                self,
                block,
                &self.column_offsets,
                self.row_size,
                self.rows_per_block))
    }

    pub fn get_block_for_row(&self, row: usize) -> Option<(RecordBlock, usize)> {
        let block_index = row / self.rows_per_block;
        let row_in_block = row % self.rows_per_block;
        self.get_block(block_index).map(|block| (block, row_in_block))
    }
}

impl<'a> Drop for Record<'a> {
    fn drop(&mut self) {
        self.record_guard.unlock(self.key);
    }
}

/// A block for a `Record`.
pub struct RecordBlock<'a> {
    #[allow(unused)]
    record: &'a Record<'a>,
    block: Block,
    column_offsets: &'a Vec<usize>,
    row_size: usize,
    rows_per_block: usize,
}

impl<'a> RecordBlock<'a> {
    fn new(record: &'a Record,
           block: Block,
           column_offsets: &'a Vec<usize>,
           row_size: usize,
           rows_per_block: usize,) -> Self {
        RecordBlock {
            record,
            block,
            column_offsets,
            row_size,
            rows_per_block
        }
    }

    pub fn get_row(&self, row: usize) -> Option<RowIter> {
        if row < self.rows_per_block {
            Some(RowIter::new(self, row))
        } else {
            None
        }
    }

    pub fn get_col(&self, col: usize) -> Option<ColIter> {
        if col < self.column_offsets.len() - 1 {
            Some(ColIter::new(self, col))
        } else {
            None
        }
    }

    /// Returns the raw value at the specified `row` and `col` if it exists.
    pub fn get_row_col(&self, row: usize, col: usize) -> Option<&[u8]> {
        let row_offset = self.row_size * row;
        let left_bound = row_offset + self.column_offsets.get(col)?;
        let right_bound = row_offset + self.column_offsets.get(col + 1)?;
        self.block.get(left_bound..right_bound)
    }

    pub fn set_row_col(&self, row: usize, col: usize, value: &[u8]) {

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

/// An iterator over the columns of a given row.
pub struct RowIter<'a> {
    block: &'a RecordBlock<'a>,
    row: usize,
    col: usize
}

impl<'a> RowIter<'a> {
    fn new(block: &'a RecordBlock<'a>, row: usize) -> Self {
        RowIter {
            block,
            row,
            col: 0
        }
    }
}

impl<'a> Iterator for RowIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        let value = self.block.get_row_col(self.row, self.col);
        self.col += 1;
        value
    }
}

/// An iterator over the rows of a given column.
pub struct ColIter<'a> {
    block: &'a RecordBlock<'a>,
    row: usize,
    col: usize
}

impl<'a> ColIter<'a> {
    fn new(block: &'a RecordBlock<'a>, col: usize) -> Self {
        ColIter {
            block,
            row: 0,
            col
        }
    }
}

impl<'a> Iterator for ColIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        let value = self.block.get_row_col(self.row, self.col);
        self.row += 1;
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
    /// ```
    /// use storage::StorageManager;
    /// let sm = StorageManager::new();
    /// sm.put("key", b"value");
    /// ```
    pub fn put(&self, key: &str, value: &[u8]) {
        self.record_guard.lock(key);

        let block_count = (value.len()  - 1) / BLOCK_SIZE + 1;
        for block_index in 0..block_count {
            let key_for_block = Self::key_for_block(key, block_index);
            let value_offset = block_index * BLOCK_SIZE;
            let value_right_bound = value_offset + min(BLOCK_SIZE, value.len() - value_offset);
            let value_for_block = &value[value_offset..value_right_bound];
            self.buffer.write(&key_for_block, value_for_block);
        }

        self.record_guard.unlock(key);
    }

    /// Writes the value to the buffer, returning a unique key that can be used to request it.
    ///
    /// # Examples
    ///
    /// ```
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
    /// ```
    /// use storage::StorageManager;
    /// let sm = StorageManager::new();
    /// sm.put("key", b"value");
    /// sm.get("key");
    /// ```
    pub fn get<'a>(&'a self, key: &'a str) -> Option<Record<'a>> {
        self.record_guard.lock(key);
        let key_for_block = Self::key_for_block(key, 0);
        if self.buffer.exists(&key_for_block) {
            Some(Record::new(key, &self.buffer, &self.record_guard))
            // When the record drops, record_guard.unlock(key) is called
        } else {
            self.record_guard.unlock(key);
            None
        }
    }

    /// Gets the value associated with `key` from the buffer and returns a `StructuredRecord` with
    /// `schema` if it exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage::StorageManager;
    /// let sm = StorageManager::new();
    /// sm.put("key", b"aaabbcccc");
    /// sm.get_structured("key", &[3, 2, 4]);
    /// ```
    pub fn get_with_schema<'a>(&'a self,
                               key: &'a str,
                               schema: &'a [usize]) -> Option<Record<'a>> {
        self.record_guard.lock(key);
        let key_for_block = Self::key_for_block(key, 0);
        if self.buffer.exists(&key_for_block) {
            Some(Record::with_schema(key, &self.buffer, &self.record_guard, schema))
            // When the record drops, record_guard.unlock(key) is called
        } else {
            self.record_guard.unlock(key);
            None
        }
    }

    /// Deletes the key-value pair.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage::StorageManager;
    /// let sm = StorageManager::new();
    /// sm.put("key", b"value");
    /// sm.delete("key");
    /// ```
    pub fn delete(&self, key: &str) {
        self.record_guard.lock(key);
        self.buffer.erase(key);
        self.record_guard.unlock(key);
    }

    fn key_for_block(key: &str, block_index: usize) -> String {
        format!("{}.{}", key, block_index)
    }
}

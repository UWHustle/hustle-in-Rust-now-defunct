extern crate memmap;
extern crate omap;

use std::sync::Mutex;
use std::mem;
use buffer::{Block, Buffer, BLOCK_SIZE};
use record_guard::{MutexRecordGuard, RecordGuard};
use std::cmp::min;
use std::ops::Deref;

const DEFAULT_BUFFER_CAPACITY: usize = 1000;
const TEMP_RECORD_PREFIX: char = '$';

/// A record that represents a key-value pair in the `StorageManager`. The value can be accessed
/// with raw byte offset, at the block level for bulk operations, or at the row/column level for
/// structured data processing.
pub struct Record<'a> {
    key: &'a str,
    buffer: &'a Buffer,
    record_guard: &'a Box<RecordGuard + Send + Sync>,
    column_offsets: Vec<usize>,
    row_size: usize,
    rows_per_block: usize
}

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

    /// Returns the number of rows in the `Record`.
    pub fn len(&self) -> usize {
        let mut tail_block_index = 0;
        while self.buffer.exists(&StorageManager::key_for_block(self.key, tail_block_index + 1)) {
            tail_block_index += 1;
        }

        let key_for_tail_block = StorageManager::key_for_block(self.key, tail_block_index);
        let rows_in_tail_block = self.buffer.get(&key_for_tail_block)
            .map(|block| block.len() / self.row_size)
            .unwrap_or(0);

        return tail_block_index * self.rows_per_block + rows_in_tail_block;
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
                self.row_size))
    }

    /// Returns the block that contains the specified `row` and the row index within that block if
    /// it exists.
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
    n_rows: usize,
}

impl<'a> RecordBlock<'a> {
    fn new(record: &'a Record,
           block: Block,
           column_offsets: &'a Vec<usize>,
           row_size: usize) -> Self {
        let n_rows = block.len() / row_size;
        RecordBlock {
            record,
            block,
            column_offsets,
            row_size,
            n_rows
        }
    }

    /// Returns an iterator over the columns of the specified `row`.
    pub fn get_row(&self, row: usize) -> Option<RowIter> {
        if row < self.n_rows {
            Some(RowIter::new(self, row))
        } else {
            None
        }
    }

    /// Returns an iterator over the rows of the specified `col`.
    pub fn get_col(&self, col: usize) -> Option<ColIter> {
        if col < self.column_offsets.len() - 1 {
            Some(ColIter::new(self, col))
        } else {
            None
        }
    }

    /// Returns the raw value at the specified `row` and `col` if it exists.
    pub fn get_row_col(&self, row: usize, col: usize) -> Option<&[u8]> {
        self.bounds_for_row_col(row, col)
            .and_then(|(left_bound, right_bound)| self.block.get(left_bound..right_bound))
    }

    /// Sets the raw value at the specified `row` and `col`. A `panic!` will occur if the `row` or
    /// `col` is out of bounds or the value is the wrong size for the schema.
    pub fn set_row_col(&self, row: usize, col: usize, value: &[u8]) {
        let (left_bound, right_bound) = self.bounds_for_row_col(row, col)
            .expect(format!("Row {} and col {} out of bounds.", row, col).as_str());

        if value.len() != right_bound - left_bound {
            panic!("Value for row {} and col {} is the wrong size ({}).",
                   row, col, value.len())
        }

        self.block.update(left_bound, value);
    }

    /// Returns the number of rows in the block.
    pub fn len(&self) -> usize {
        self.block.len() / self.row_size
    }

    /// Returns the left and right bounds in the block for the specified `row` and `col`.
    fn bounds_for_row_col(&self, row: usize, col: usize) -> Option<(usize, usize)> {
        if row < self.n_rows && col < self.column_offsets.len() - 1 {
            let row_offset = self.row_size * row;
            let left_bound = row_offset + self.column_offsets.get(col)?;
            let right_bound = row_offset + self.column_offsets.get(col + 1)?;
            Some((left_bound, right_bound))
        } else {
            None
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
    /// sm.put("key_put", b"value");
    /// assert_eq!(&sm.get("key_put").unwrap().get_block(0).unwrap()[0..5], b"value");
    /// sm.delete("key_put");
    /// ```
    ///
    /// TODO: If the length of value is smaller than the existing value, we need to remove the
    /// blocks from the end.
    pub fn put(&self, key: &str, value: &[u8]) {
        self.record_guard.lock(key);

        if value.is_empty() {
            self.buffer.write(&Self::key_for_block(key, 0), value);
        } else {
            let block_count = (value.len() - 1) / BLOCK_SIZE + 1;
            for block_index in 0..block_count {
                let key_for_block = Self::key_for_block(key, block_index);
                let value_offset = block_index * BLOCK_SIZE;
                let value_right_bound = value_offset + min(BLOCK_SIZE, value.len() - value_offset);
                let value_for_block = &value[value_offset..value_right_bound];
                self.buffer.write(&key_for_block, value_for_block);
            }
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
    /// assert_eq!(&sm.get(&key).unwrap().get_block(0).unwrap()[0..5], b"value");
    /// sm.delete(&key);
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

    /// Appends the value to the existing value associated with the specified `key`.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage::StorageManager;
    /// let sm = StorageManager::new();
    /// sm.append("key_append", b"a");
    /// assert_eq!(&sm.get("key_append").unwrap().get_block(0).unwrap()[0..1], b"a");
    /// sm.append("key_append", b"bb");
    /// assert_eq!(&sm.get("key_append").unwrap().get_block(0).unwrap()[0..3], b"abb");
    /// sm.delete("key_append");
    /// ```
    pub fn append(&self, key: &str, value: &[u8]) {
        if value.len() > BLOCK_SIZE {
            panic!("Value size {} is too large for a single block.", value.len());
        }

        self.record_guard.lock(key);

        let tail_block_index = Self::tail_block_index(key, &self.buffer);
        let key_for_tail_block = Self::key_for_block(key, tail_block_index);

        if let Some(block) = self.buffer.get(&key_for_tail_block) {
            if block.len() + value.len() <= BLOCK_SIZE {
                // The value can be appended to the tail block.
                block.append(value);
            } else {
                // The value will not fit in the tail block, so we create a new block at the end.
                self.buffer.write(&Self::key_for_block(key, tail_block_index + 1), value);
            }
        } else {
            // The key-value pair does not exist. Create it and initialize it with the value.
            self.buffer.write(&key_for_tail_block, value);
        }

        self.record_guard.unlock(key);
    }

    pub fn extend(&self, key: &str, value: &[u8], row_size: usize) {
        if row_size > BLOCK_SIZE {
            panic!("Row size {} is too large for a single block.", row_size);
        }

        self.record_guard.lock(key);

        let mut tail_block_index = Self::tail_block_index(key, &self.buffer);
        let mut offset = 0;

        // Extend the tail block for the specified key with the value, if it exists.
        if let Some(block) = self.buffer.get(&Self::key_for_block(key, tail_block_index)) {

            let available_size = BLOCK_SIZE - block.len();
            if available_size >= row_size {
                // The tail block can be extended with the value.
                let size = min(available_size - available_size % row_size, value.len());
                block.append(&value[offset..offset + size]);
                offset += size;
            }

            tail_block_index += 1;
        }

        // Continue to extend, breaking the value up into blocks if necessary.
        let available_size = BLOCK_SIZE - BLOCK_SIZE % row_size;
        while offset < value.len() {
            let size = min(available_size, value.len() - offset);
            let value_block = &value[offset..offset + size];
            self.buffer.write(&Self::key_for_block(key, tail_block_index), value_block);
            tail_block_index += 1;
            offset += size;
        }

        self.record_guard.unlock(key);
    }

    /// Gets the value associated with `key` from the buffer and returns a `Record` if it exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage::StorageManager;
    /// let sm = StorageManager::new();
    /// sm.put("key_get", b"value");
    /// assert_eq!(&sm.get("key_get").unwrap().get_block(0).unwrap()[0..5], b"value");
    /// assert!(sm.get("nonexistent_key").is_none());
    /// sm.delete("key_get");
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

    /// Returns a snapshot of the value associated with `key` as a contiguous vector of bytes.
    pub fn get_concat(&self, key: &str) -> Option<Vec<u8>> {
        let record = self.get(key);
        record.map(|r| {
            let mut value = vec![];
            for block in r.blocks() {
                value.extend_from_slice(&block[..block.len()]);
            }
            value
        })
    }

    /// Gets the value associated with `key` from the buffer and returns a `StructuredRecord` with
    /// `schema` if it exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage::StorageManager;
    /// let sm = StorageManager::new();
    /// sm.put("key_get_with_schema", b"abbcdd");
    /// assert_eq!(&sm.get_with_schema("key_get_with_schema", &[1, 2]).unwrap()
    ///     .get_block(0).unwrap()
    ///     .get_row_col(0, 0).unwrap(), &b"a");
    /// sm.delete("key_get_with_schema");
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
    /// sm.put("key_delete", b"value");
    /// sm.delete("key_delete");
    /// ```
    pub fn delete(&self, key: &str) {
        self.record_guard.lock(key);

        let mut block_index = 0;
        loop {
            let key_for_block = Self::key_for_block(key, block_index);
            if !self.buffer.exists(&key_for_block) {
                break;
            }
            self.buffer.erase(&key_for_block);
            block_index += 1;
        }

        self.record_guard.unlock(key);
    }

    /// Returns the key for the block at the specified `block_index`.
    fn key_for_block(key: &str, block_index: usize) -> String {
        format!("{}${}", key, block_index)
    }

    fn tail_block_index(key: &str, buffer: &Buffer) -> usize {
        let mut tail_block_index = 0;
        while buffer.exists(&Self::key_for_block(key, tail_block_index + 1)) {
            tail_block_index += 1;
        }
        tail_block_index
    }
}

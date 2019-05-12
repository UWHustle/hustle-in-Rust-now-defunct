use std::sync::{Arc, Condvar, Mutex};
use std::path::{PathBuf, Path};
use std::fs::OpenOptions;
use memmap::MmapMut;
use std::{mem, slice, fs, ptr};
use std::ops::Deref;
use std::io::Write;

const BLOCK_SIZE: usize = 1024;

#[derive(Clone)]
pub struct RelationalBlockHeader {
    n_rows: *mut usize,
    n_cols: *mut usize,
    row_size: *mut usize,
    row_capacity: *mut usize,
    schema: *mut usize
}

impl RelationalBlockHeader {
    fn new(schema: &[usize], destination: &mut [u8]) -> Self {
        // Blocks must have a schema.
        debug_assert!(!schema.is_empty());

        let n_rows = 0;
        let n_cols = schema.len();
        let row_size: usize = schema.iter().sum();
        let row_capacity = 0;

        // The destination slice should be large enough to accommodate the header.
        debug_assert!(mem::size_of_val(&n_rows)
            + mem::size_of_val(&n_cols)
            + mem::size_of_val(&row_size)
            + mem::size_of_val(&row_capacity)
            + schema.len() * mem::size_of_val(&schema[0]) <= destination.len());

        let header = Self::try_from_slice(destination).unwrap();

        unsafe {
            *header.n_rows = n_rows;
            *header.n_cols = n_cols;
            *header.row_size = row_size;
            *header.row_capacity = row_capacity;
            let schema_slice = slice::from_raw_parts_mut(header.schema, n_cols);
            schema_slice.copy_from_slice(schema);
        }

        header
    }

    fn try_from_slice(source: &mut [u8]) -> Option<Self> {
        let mut n_rows_raw = ptr::null_mut();
        let mut n_cols_raw = ptr::null_mut();
        let mut row_size_raw = ptr::null_mut();
        let mut row_capacity_raw = ptr::null_mut();
        let mut schema_raw = ptr::null_mut();

        let mut offset = 0;
        n_rows_raw = source
            .get_mut(offset..)?
            .as_mut_ptr() as *mut usize;
        offset += mem::size_of_val(&n_rows_raw);

        n_cols_raw = source
            .get_mut(offset..)?
            .as_mut_ptr() as *mut usize;
        offset += mem::size_of_val(&n_cols_raw);

        row_size_raw = source
            .get_mut(offset..)?
            .as_mut_ptr() as *mut usize;
        offset += mem::size_of_val(&row_size_raw);

        row_capacity_raw = source
            .get_mut(offset..)?
            .as_mut_ptr() as *mut usize;
        offset += mem::size_of_val(&row_capacity_raw);

        schema_raw = source
            .get_mut(offset..)?
            .as_mut_ptr() as *mut usize;

        Some(RelationalBlockHeader {
            n_rows: n_rows_raw,
            n_cols: n_cols_raw,
            row_size: row_size_raw,
            row_capacity: row_capacity_raw,
            schema: schema_raw
        })
    }

    fn size(&self) -> usize {
        mem::size_of_val(&self.n_rows)
            + mem::size_of_val(&self.n_cols)
            + mem::size_of_val(&self.row_size)
            + mem::size_of_val(&self.row_capacity)
            + unsafe { *self.n_cols } * mem::size_of_val(&self.schema)
    }

    fn get_n_rows(&self) -> usize {
        unsafe { *self.n_rows }
    }

    fn get_n_cols(&self) -> usize {
        unsafe { *self.n_cols }
    }

    fn get_row_size(&self) -> usize {
        unsafe { *self.row_size }
    }

    fn get_row_capacity(&self) -> usize {
        unsafe { *self.row_capacity }
    }

    fn get_schema(&self) -> &[usize] {
        unsafe { slice::from_raw_parts(self.schema, *self.n_cols) }
    }

    fn set_n_rows(&self, n_rows: usize) {
        unsafe { *self.n_rows = n_rows };
    }

    fn set_row_capacity(&self, row_capacity: usize) {
        unsafe { *self.row_capacity = row_capacity };
    }
}

/// The unit of storage and replacement in the cache. A `RelationalBlock` is a horizontal partition
/// of a `PhysicalRelation`.
pub struct RelationalBlock {
    header: RelationalBlockHeader,
    data: *mut u8,
    mmap: Arc<MmapMut>,
    rc: Arc<(Mutex<u64>, Condvar)>
}

impl RelationalBlock {
    /// Creates a new `RelationalBlock`, backed by a file on storage. If the file path for the
    /// specified `key` already exists, it will be overwritten.
    pub fn new(key: &str, schema: &[usize]) -> Self {
        let path = Self::file_path(key);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .expect("Error opening file.");

        file.set_len(BLOCK_SIZE as u64);

        let mut mmap = unsafe {
            MmapMut::map_mut(&file)
                .expect("Error memory-mapping file.")
        };

        let header = RelationalBlockHeader::new(schema, &mut mmap);
        let header_size = header.size();
        header.set_row_capacity((BLOCK_SIZE - header_size) / header.get_row_size());
        let data = mmap[header_size..].as_mut_ptr();
        
        RelationalBlock {
            header,
            data,
            mmap: Arc::new(mmap),
            rc: Arc::new((Mutex::new(1), Condvar::new()))
        }
    }

    /// Loads a `RelationalBlock` from storage using the specified `path`. Returns an `Option`
    /// containing the `RelationalBlock` if it exists, otherwise `None`.
    pub fn try_from_file(path: &Path) -> Option<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .ok()?;
        file.set_len(BLOCK_SIZE as u64).ok()?;

        let mut mmap = unsafe { MmapMut::map_mut(&file).ok()? };
        let header = RelationalBlockHeader::try_from_slice(&mut mmap)?;
        let data = mmap[header.size()..].as_mut_ptr();

        Some(RelationalBlock {
            header,
            data,
            mmap: Arc::new(mmap),
            rc: Arc::new((Mutex::new(1), Condvar::new()))
        })
    }

    /// Returns the number of rows in the `RelationalBlock`.
    pub fn get_n_rows(&self) -> usize {
        self.header.get_n_rows()
    }

    pub fn get_row_size(&self) -> usize {
        self.header.get_row_size()
    }

    /// Returns the total number of rows that can fit into the `RelationalBlock`.
    pub fn get_row_capacity(&self) -> usize {
        self.header.get_row_capacity()
    }

    pub fn get_schema(&self) -> &[usize] {
        self.header.get_schema()
    }

    /// Returns the raw value at the specified `row` and `col` if it exists.
    pub fn get_row_col(&self, row: usize, col: usize) -> Option<&[u8]> {
        self.position_of_row_col(row, col)
            .and_then(|(offset, size)| self.data_as_slice().get(offset..offset + size))
    }

    /// Sets the raw value at the specified `row` and `col`. A `panic!` will occur if the `row` or
    /// `col` is out of bounds or the value is the wrong size for the schema.
    pub fn set_row_col(&self, row: usize, col: usize, value: &[u8]) {
        let (offset, size) = self.position_of_row_col(row, col)
            .expect(format!("Row {} and col {} out of bounds.", row, col).as_str());

        assert_eq!(value.len(), size, "Value for row {} and col {} is the wrong size.", row, col);

        self.data_as_slice_mut()[offset..offset + size].copy_from_slice(value);
    }

    /// Returns a `RowBuilder` that can be used to construct a new row by pushing values.
    pub fn insert_row(&mut self) -> RowBuilder {
        assert!(self.get_n_rows() < self.header.get_row_capacity());
        let mut schema = vec![];
        schema.extend_from_slice(self.get_schema());
        let row_builder = RowBuilder::new(self.get_n_rows(), schema, self.clone());
        self.header.set_n_rows(self.header.get_n_rows() + 1);
        row_builder
    }

    pub fn bulk_read(&self) -> Vec<u8> {
        let mut result = vec![];
        for row_i in 0..self.header.get_n_rows() {
            for col_i in 0..self.header.get_n_cols() {
                self.get_row_col(row_i, col_i)
                    .map(|value| result.extend_from_slice(value));
            }
        }
        result
    }

    pub fn bulk_write(&mut self, value: &[u8]) {
        assert!(value.len() <= self.header.get_row_size() * self.header.get_row_capacity());
        self.clear();
        let n_rows = value.len() / self.header.get_row_size();
        let schema = self.header.get_schema();
        let mut offset = 0;
        for row_i in 0..n_rows {
            for col_i in 0..self.header.get_n_cols() {
                let size = schema[col_i];
                self.set_row_col(row_i, col_i, &value[offset..offset + size]);
                offset += size;
            }
        }
    }

    pub fn clear(&self) {
        self.header.set_n_rows(0);
    }

    pub fn get_reference_count(&self) -> &Arc<(Mutex<u64>, Condvar)> {
        &self.rc
    }

    /// Returns the raw data of the block, excluding the header, as a mutable slice of bytes.
    fn data_as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.data, BLOCK_SIZE - self.header.size()) }
    }

    /// Returns the raw data of the block, excluding the header, as a mutable slice of bytes.
    fn data_as_slice_mut(&self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.data, BLOCK_SIZE - self.header.size()) }
    }

    /// Returns the offset in the block and the size for the specified `row` and `col`.
    fn position_of_row_col(&self, row: usize, col: usize) -> Option<(usize, usize)> {
        let schema = self.get_schema();
        if row < self.get_n_rows() && col < self.get_n_rows() {
            let row_offset = self.get_row_size() * row;
            let col_offset: usize = schema.get(..col)?.iter().sum();
            let offset = row_offset + col_offset;
            let size = schema.get(col)?.clone();
            Some((offset, size))
        } else {
            None
        }
    }

    /// Returns the formatted file path for the specified `key`.
    fn file_path(key: &str) -> PathBuf {
        let mut path = PathBuf::from(key);
        path.set_extension("hsl");
        path
    }
}

impl Clone for RelationalBlock {
    fn clone(&self) -> Self {
        let mut rc_guard = self.rc.0.lock().unwrap();
        *rc_guard += 1;
        RelationalBlock {
            header: self.header.clone(),
            data: self.data,
            mmap: self.mmap.clone(),
            rc: self.rc.clone()
        }
    }
}

impl Drop for RelationalBlock {
    fn drop(&mut self) {
        // Decrement the reference count when Value is dropped.
        let &(ref rc_lock, ref cvar) = &*self.rc;
        let mut rc_guard = rc_lock.lock().unwrap();
        *rc_guard -= 1;
        cvar.notify_all();
    }
}

pub struct RowBuilder {
    row: usize,
    col: usize,
    schema: Vec<usize>,
    block: RelationalBlock
}

impl RowBuilder {
    pub fn new(row: usize, schema: Vec<usize>, block: RelationalBlock) -> Self {
        RowBuilder {
            row,
            col: 0,
            schema,
            block
        }
    }

    pub fn push(&mut self, value: &[u8]) {
        assert!(self.col < self.schema.len());
        assert_eq!(value.len(), self.schema[self.col]);
        self.block.set_row_col(self.row, self.col, value);
        self.col += 1;
    }
}

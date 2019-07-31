use std::slice;
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};

use memmap::MmapMut;

use buffer_manager::BufferManager;
use block::{Header, BLOCK_SIZE, BitMap};
use std::ops::{DerefMut, Deref};

#[derive(Clone)]
struct RawSlice {
    data: *mut u8,
    len: usize,
}

impl RawSlice {
    fn new(s: &mut [u8]) -> Self {
        RawSlice {
            data: s.as_mut_ptr(),
            len: s.len(),
        }
    }
}

impl Deref for RawSlice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { slice::from_raw_parts(self.data, self.len) }
    }
}

impl DerefMut for RawSlice {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { slice::from_raw_parts_mut(self.data, self.len) }
    }
}

/// The unit of storage and replacement in the cache. A `RelationalBlock` is a horizontal partition
/// of a `PhysicalRelation`.
pub struct RelationalBlock {
    header: Header<RawSlice>,
    valid: BitMap<RawSlice>,
    ready: BitMap<RawSlice>,
    data: RawSlice,
    mmap: Arc<MmapMut>,
    rc: Arc<(Mutex<u64>, Condvar)>
}

impl RelationalBlock {
    /// Creates a new `RelationalBlock`, backed by a file on storage. If the file path for the
    /// specified `key` already exists, it will be overwritten.
    pub fn new(key: &str, schema: &[usize], buffer_manager: &BufferManager) -> Self {
        let mut options = OpenOptions::new();
        options
            .read(true)
            .write(true)
            .create(true);

        let file = buffer_manager.open(key, &options)
            .expect("Error opening file.");

        file.set_len(BLOCK_SIZE as u64)
            .expect("Error setting length of file.");

        let mut mmap = unsafe {
            MmapMut::map_mut(&file)
                .expect("Error memory-mapping file.")
        };

        let header = Header::new(schema, RawSlice::new(&mut mmap));

        Self::with_header(header, mmap)
    }

    /// Loads a `RelationalBlock` from storage using the specified `path`. Returns an `Option`
    /// containing the `RelationalBlock` if it exists, otherwise `None`.
    pub fn try_from_file(path: &Path) -> Option<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .ok()?;
        file.set_len(BLOCK_SIZE as u64).ok()?;

        let mut mmap = unsafe { MmapMut::map_mut(&file).ok()? };

        let header = Header::with_buf(RawSlice::new(&mut mmap));

        Some(Self::with_header(header, mmap))
    }

    fn with_header(header: Header<RawSlice>, mut mmap: MmapMut) -> Self {
        let bitmap_size = header.get_bitmap_size();

        let valid_start = header.size();
        let ready_start = valid_start + bitmap_size;
        let data_start = ready_start + bitmap_size;

        let valid = BitMap::new(RawSlice::new(&mut mmap[valid_start..ready_start]));
        let ready = BitMap::new(RawSlice::new(&mut mmap[ready_start..data_start]));
        let data = RawSlice::new(&mut mmap[data_start..]);

        RelationalBlock {
            header,
            valid,
            ready,
            data,
            mmap: Arc::new(mmap),
            rc: Arc::new((Mutex::new(1), Condvar::new()))
        }
    }

    /// Returns the number of rows in the `RelationalBlock`.
    pub fn get_n_rows(&self) -> usize {
        self.header.get_n_rows()
    }

    /// Returns the number of columns in the `RelationalBlock`.
    pub fn get_n_cols(&self) -> usize {
        self.header.get_n_cols()
    }

    /// Returns the size of one row.
    pub fn get_row_size(&self) -> usize {
        self.header.get_row_size()
    }

    /// Returns the total number of rows that can fit into the `RelationalBlock`.
    pub fn get_row_capacity(&self) -> usize {
        self.header.get_row_capacity()
    }

    pub fn get_schema(&self) -> Vec<usize> {
        self.header.get_schema()
    }

    /// Returns the raw value at the specified `row` and `col` if it exists.
    pub fn get_row_col(&self, row: usize, col: usize) -> Option<&[u8]> {
        unimplemented!()
    }

    /// Sets the raw value at the specified `row` and `col`. A `panic!` will occur if the `row` or
    /// `col` is out of bounds or the value is the wrong size for the schema.
    pub fn set_row_col(&self, row: usize, col: usize, value: &[u8]) {
        unimplemented!()
    }

    /// Returns a `RowBuilder` that can be used to construct a new row by pushing values.
    pub fn insert_row(&mut self) -> RowBuilder {
        unimplemented!()
    }

    /// Deletes the specified `row`. To maintain packing, the last row of the block is moved from
    /// the end to the deleted position.
    pub fn delete_row(&mut self, row: usize) {
        unimplemented!()
    }

    /// Returns the entire data of the `RelationalBlock`, concatenated into a vector of bytes in
    /// row-major format.
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

    /// Overwrites the entire `RelationalBlock` with the `value`, which must be in row-major format.
    pub fn bulk_write(&mut self, value: &[u8]) {
        assert!(value.len() <= self.header.get_row_size() * self.header.get_row_capacity());
        self.clear();
        let n_rows = value.len() / self.header.get_row_size();
        self.header.set_n_rows(n_rows);
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

    /// Clears all the rows from the `RelationalBlock`, but does not remove it from storage.
    pub fn clear(&mut self) {
        self.header.set_n_rows(0);
    }

    pub fn get_reference_count(&self) -> &Arc<(Mutex<u64>, Condvar)> {
        &self.rc
    }
}

impl Clone for RelationalBlock {
    fn clone(&self) -> Self {
        let mut rc_guard = self.rc.0.lock().unwrap();
        *rc_guard += 1;
        RelationalBlock {
            header: self.header.clone(),
            valid: self.valid.clone(),
            ready: self.ready.clone(),
            data: self.data.clone(),
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

/// A utility for constructing a row in a `RelationalBlock`.
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

    /// Push a new cell into the row. A `panic!` will occur if the row is already full or the value
    /// is the wrong size for the schema.
    pub fn push(&mut self, value: &[u8]) {
        assert!(self.col < self.schema.len());
        assert_eq!(value.len(), self.schema[self.col]);
        self.block.set_row_col(self.row, self.col, value);
        self.col += 1;
    }
}

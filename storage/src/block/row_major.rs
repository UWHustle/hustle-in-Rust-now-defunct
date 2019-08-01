use std::fs::OpenOptions;
use std::path::Path;

use memmap::MmapMut;

use buffer_manager::BufferManager;
use block::{Header, BLOCK_SIZE, BitMap, RawSlice};

/// A `RowMajorBlock` is a horizontal partition of a `PhysicalRelation` in row-major order.
pub struct RowMajorBlock {
    header: Header,
    valid: BitMap<RawSlice>,
    ready: BitMap<RawSlice>,
    data: RawSlice,
    column_offsets: Vec<usize>,
    mmap: MmapMut,
}

impl RowMajorBlock {
    /// Creates a new `RowMajorBlock`, backed by a file on storage. If the file path for the
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

        let header = Header::new(schema, &mut mmap);

        Self::with_header(header, mmap)
    }

    /// Loads a `RowMajorBlock` from storage using the specified `path`. Returns an `Option`
    /// containing the `RowMajorBlock` if it exists, otherwise `None`.
    pub fn try_from_file(path: &Path) -> Option<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .ok()?;
        file.set_len(BLOCK_SIZE as u64).ok()?;

        let mut mmap = unsafe { MmapMut::map_mut(&file).ok()? };

        let header = Header::with_buf(&mut mmap);

        Some(Self::with_header(header, mmap))
    }

    fn with_header(header: Header, mut mmap: MmapMut) -> Self {
        let bitmap_size = header.get_bitmap_size();

        let valid_start = header.size();
        let ready_start = valid_start + bitmap_size;
        let data_start = ready_start + bitmap_size;

        let valid = BitMap::new(RawSlice::new(&mut mmap[valid_start..ready_start]));
        let ready = BitMap::new(RawSlice::new(&mut mmap[ready_start..data_start]));
        let data = RawSlice::new(&mut mmap[data_start..]);

        let column_offsets = header.get_schema().iter()
            .scan(0, |state, &col_size| {
                let offset = *state;
                *state += col_size;
                Some(offset)
            })
            .collect();

        RowMajorBlock {
            header,
            valid,
            ready,
            data,
            column_offsets,
            mmap,
        }
    }

    /// Returns the number of columns in the `RowMajorBlock`.
    pub fn get_n_cols(&self) -> usize {
        self.header.get_n_cols()
    }

    /// Returns the size of one row.
    pub fn get_row_size(&self) -> usize {
        self.header.get_row_size()
    }

    /// Returns the total number of rows that can fit into the `RowMajorBlock`.
    pub fn get_row_capacity(&self) -> usize {
        self.header.get_row_capacity()
    }

    pub fn get_schema(&self) -> Vec<usize> {
        self.header.get_schema()
    }

    pub fn project<F>(&self, cols: &[usize], f: F) where F: Fn(&[&[u8]]) {
        let row_offsets = self.valid.iter()
            .zip(self.ready.iter())
            .zip((0..).step_by(self.get_row_size()))
            .filter(|((valid, ready), _)| *valid && *ready)
            .map(|(_, offset)| offset);

        let mut row_buf = vec![&[] as &[u8]; cols.len()];
        let schema = self.get_schema();
        for offset in row_offsets {
            for (i, col) in cols.iter().enumerate() {
                let start = offset + self.column_offsets[*col];
                let end = start + schema[*col];
                row_buf[i] = &self.data[start..end];
            }
            f(&row_buf)
        }
    }

    /// Inserts a new row into the block. The row is inserted into the first available slot.
    pub fn insert(&mut self, row: &[&[u8]]) {
        unimplemented!()
    }

    /// Deletes each row in the block where `filter` called on that row returns true.
    pub fn delete<F>(&mut self, filter: F) where F: Fn(&[&[u8]]) -> bool {
        unimplemented!()
    }

    /// Clears all the rows from the `RowMajorBlock`, but does not remove it from storage.
    pub fn clear(&mut self) {
        self.header.get_n_rows_guard().set(0);
    }

    /// Returns the entire data of the `RowMajorBlock`, concatenated into a vector of bytes in
    /// row-major format.
    pub fn bulk_read(&self) -> Vec<u8> {
        unimplemented!()
    }

    /// Overwrites the entire `RowMajorBlock` with the `value`, which must be in row-major format.
    pub fn bulk_write(&self, value: &[u8]) {
        unimplemented!()
    }
}

pub struct ProjectIter<'a> {
    cols: &'a [usize],
}

impl<'a> ProjectIter<'a> {
    fn new(cols: &'a [usize]) -> Self {
        ProjectIter {
            cols,
        }
    }
}



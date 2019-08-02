use std::io::{Cursor, Write};

use memmap::MmapMut;

use block::{BitMap, Header, RawSlice};

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
    pub fn new(schema: &[usize], mut mmap: MmapMut) -> Self {
        Self::with_header(Header::new(schema, &mut mmap), mmap)
    }

    /// Loads a `RowMajorBlock` from storage using the specified `path`. Returns an `Option`
    /// containing the `RowMajorBlock` if it exists, otherwise `None`.
    pub fn with_buf(mut mmap: MmapMut) -> Self {
        Self::with_header(Header::with_buf(&mut mmap), mmap)
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

    pub fn is_full(&self) -> bool {
        self.header.get_n_rows_guard().get() == self.get_row_capacity()
    }

    pub fn rows<F>(&self, f: F) where F: Fn(&[&[u8]]) {
        let cols: Vec<usize> = (0..self.get_n_cols()).collect();
        self.project(&cols, f)
    }

    pub fn project<F>(&self, cols: &[usize], f: F) where F: Fn(&[&[u8]]) {
        let schema = self.get_schema();
        let mut row_buf = vec![&[] as &[u8]; cols.len()];

        let rows = self.row_offsets()
            .filter(|(_, _, valid, ready)| *valid && *ready);

        for (_, offset, _, _) in rows {
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
        let schema = self.get_schema();
        assert!(
            row.len() == schema.len()
                && row.iter()
                    .zip(schema)
                    .all(|(insert_col, col_len)| insert_col.len() == col_len),
            "Row has incorrect schema",
        );

        let offset = {
            // Determine whether the block is at capacity. If there is space to insert a row,
            // increment n_rows. Access to n_rows_guard is scoped so we don't hold the lock any
            // longer than necessary.
            let mut n_rows_guard = self.header.get_n_rows_guard();
            let n_rows = n_rows_guard.get();
            if n_rows == self.get_row_capacity() {
                panic!("Block is already at capacity");
            }
            n_rows_guard.set(n_rows + 1);

            // Find the position in the block to insert the row.
            self.row_offsets()
                .find(|(_, _, valid, ready)| !*valid && *ready)
                .unwrap()
                .1
        };

        // Write the new row to the block.
        let mut cursor = Cursor::new(&mut self.data[offset..]);
        for &col in row {
            cursor.write(col).unwrap();
        }
    }

    /// For each row in the block, deletes the row if `filter` called on that row returns true.
    pub fn delete<F>(&mut self, filter: F) where F: Fn(&[&[u8]]) -> bool {
        let schema = self.get_schema();
        let mut row_buf = vec![&[] as &[u8]; schema.len()];

        let rows = (0..self.get_row_capacity())
            .zip((0..).step_by(self.get_row_size()));

        for (row_i, offset) in rows {
            if self.valid.get_unchecked(row_i) && self.ready.get_unchecked(row_i) {
                for (col, col_size) in schema.iter().enumerate() {
                    let start = offset + self.column_offsets[col];
                    let end = start + *col_size;
                    row_buf[col] = &self.data[start..end];
                }

                if filter(&row_buf) {
                    let mut n_rows_guard = self.header.get_n_rows_guard();
                    self.valid.set_unchecked(row_i, false);
                    let n_rows = n_rows_guard.get() - 1;
                    n_rows_guard.set(n_rows);
                }
            }
        }
    }

    /// Clears all the rows from the `RowMajorBlock`, but does not remove it from storage.
    pub fn clear(&mut self) {
        let mut n_rows_guard = self.header.get_n_rows_guard();
        self.valid.set_all(false);
        self.ready.set_all(true);
        n_rows_guard.set(0);
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

    fn row_offsets(&self) -> impl Iterator<Item = (usize, usize, bool, bool)> + '_ {
        self.valid.iter()
            .zip(self.ready.iter())
            .zip((0..).step_by(self.get_row_size()))
            .enumerate()
            .map(|(row_i, ((valid, ready), offset))| (row_i, offset, valid, ready))
    }
}

use std::io::Cursor;
use std::iter::FromIterator;
use std::slice;
use std::sync::{Mutex, MutexGuard};

use bit_vec::BitVec;
use memmap::MmapMut;

use hustle_types::{Bits, HustleType};

pub struct RowMask {
    bits: BitVec
}

#[derive(Serialize, Deserialize)]
struct Header {
    col_sizes: Vec<usize>,
    n_flags: usize,
}

struct Metadata {
    n_rows: Mutex<usize>,
    n_cols: usize,
    row_cap: usize,
    col_offsets: Vec<usize>,
    bits_type: Bits,
}

pub struct InsertGuard<'a> {
    n_rows: MutexGuard<'a, usize>,
    block: &'a ColumnMajorBlock,
}

impl<'a> InsertGuard<'a> {
    pub fn is_full(&self) -> bool {
        *self.n_rows == self.block.metadata.row_cap
    }

    pub fn insert_row(&mut self) -> impl Iterator<Item = &mut [u8]> {
        // Find the index of the first row with the state (!valid, ready).
        let (row_i, _) = self.block.get_valid_flag_for_rows()
            .zip(self.block.get_ready_flag_for_rows())
            .enumerate()
            .find(|&(_, (valid, ready))| !valid && ready)
            .expect("Cannot insert a row into a full block");

        // Increment the number of rows.
        *self.n_rows += 1;

        // Set the row state to (valid, ready).
        self.block.set_valid_flag_for_row(row_i, true);

        // Return an iterator over mutable references to the buffers of this row.
        self.block.get_row_mut(row_i)
    }

    pub fn into_insert_rows(mut self) -> impl Iterator<Item = impl Iterator<Item = &'a mut [u8]>> {
        // Build a mask where the "on" bits represent the indices of the rows with state
        // (!valid, ready).
        let bits = BitVec::from_iter(
            self.block.get_valid_flag_for_rows()
                .zip(self.block.get_ready_flag_for_rows())
                .map(|(valid, ready)| !valid && ready)
        );
        let mask = RowMask { bits };

        // Iterate over the rows with this mask.
        self.block.get_rows_with_mask_mut(mask)
            .map(move |(row_i, row)| {
                // Increment the number of rows.
                *self.n_rows += 1;

                // Set the row state to (valid, ready).
                self.block.set_valid_flag_for_row(row_i, true);

                // Return an iterator over mutable references to the buffers of this row.
                row
            })
    }
}

pub struct ColumnMajorBlock {
    header: Header,
    metadata: Metadata,
    data: *mut u8,
    _mmap: MmapMut,
}

impl ColumnMajorBlock {
    pub fn new(col_sizes: Vec<usize>, n_flags: usize, mut mmap: MmapMut) -> Self {
        assert!(!col_sizes.is_empty(), "Cannot create a block with no columns");

        let header = Header { col_sizes, n_flags };

        let data_offset = {
            let mut cursor = Cursor::new(mmap.as_mut());
            serde_json::to_writer(&mut cursor, &header).unwrap();
            cursor.position() as usize
        };

        let block = Self::with_header(header, data_offset, mmap);

        block.delete_rows();

        block
    }

    pub fn from_buf(mut mmap: MmapMut) -> Self {
        let (header, data_offset) = {
            let mut cursor = Cursor::new(mmap.as_mut());
            let header = serde_json::from_reader(&mut cursor).unwrap();
            (header, cursor.position() as usize)
        };

        Self::with_header(header, data_offset, mmap)
    }

    fn with_header(mut header: Header, data_offset: usize, mut mmap: MmapMut) -> Self {
        let n_rows = Mutex::new(0);
        let n_cols = header.col_sizes.len();

        let (data, data_len) = {
            let data = &mut mmap[data_offset..];
            (data.as_mut_ptr(), data.len())
        };

        // Include 2 bits to signify "valid" and "ready" states.
        let bits_len = header.n_flags + 2;

        // Append a hidden column to the end of the schema to store the bit flags.
        let bits_type = Bits::new(bits_len);
        header.col_sizes.push(bits_type.byte_len());

        let row_size = header.col_sizes.iter().sum::<usize>() + bits_type.byte_len();
        let row_cap = data_len / row_size;

        let col_offsets = header.col_sizes.iter()
            .scan(0, |state, &col_size| {
                let col_offset = *state;
                *state = col_offset + row_cap * col_size;
                Some(col_offset)
            })
            .collect::<Vec<usize>>();

        let metadata = Metadata {
            n_rows,
            n_cols,
            row_cap,
            col_offsets,
            bits_type,
        };

        ColumnMajorBlock {
            header,
            metadata,
            data,
            _mmap: mmap,
        }
    }

    pub fn project<'a>(
        &'a self,
        cols: &'a [usize]
    ) -> impl Iterator<Item = impl Iterator<Item = &[u8]>> + 'a {
        let bits = BitVec::from_iter(
            self.get_valid_flag_for_rows()
                .zip(self.get_ready_flag_for_rows())
                .map(|(valid, ready)| valid && ready)
        );
        let mask = RowMask { bits };

        self.project_with_mask(cols, mask)
    }

    pub fn project_with_mask<'a>(
        &'a self,
        cols: &'a [usize],
        mask: RowMask,
    ) -> impl Iterator<Item = impl Iterator<Item = &[u8]>> + 'a {
        (0..self.metadata.row_cap)
            .zip(mask.bits.into_iter())
            .filter(|&(_, bit)| bit)
            .map(move |(row_i, _)|
                cols.iter().map(move |&col_i| self.get_row_col(row_i, col_i))
            )
    }

    pub fn delete_rows(&self) {
        self.set_valid_flag_for_rows(false);
        self.set_ready_flag_for_rows(true);
        *self.metadata.n_rows.lock().unwrap() = 0;
    }

    pub fn delete_rows_with_mask(&self, mask: RowMask) {
        let row_is = mask.bits.iter()
            .enumerate()
            .filter(|&(_, bit)| bit);

        for (row_i, _) in row_is {
            self.set_valid_flag_for_row(row_i, false);
            self.set_ready_flag_for_row(row_i, true);
            *self.metadata.n_rows.lock().unwrap() -= 1;
        }
    }

    pub fn update_col(&self, col_i: usize, value: &[u8]) {
        for buf in self.get_col_mut(col_i) {
            buf.copy_from_slice(value);
        }
    }

    pub fn update_col_with_mask(&self, col_i: usize, value: &[u8], mask: RowMask) {
        let bufs = self.get_col_mut(col_i)
            .zip(mask.bits.iter())
            .filter(|&(_, bit)| bit);

        for (buf, _) in bufs {
            buf.copy_from_slice(value);
        }
    }

    pub fn filter_col(&self, col_i: usize, f: impl Fn(&[u8]) -> bool) -> RowMask {
        let bits = BitVec::from_iter(
            self.get_valid_flag_for_rows()
                .zip(self.get_ready_flag_for_rows())
                .zip(self.get_col_mut(col_i))
                .map(|((valid, ready), buf)| valid && ready && f(buf))
        );

        RowMask { bits }
    }

    pub fn insert_guard(&self) -> InsertGuard {
        InsertGuard { n_rows: self.metadata.n_rows.lock().unwrap(), block: self }
    }

    fn get_row_col_mut(&self, row_i: usize, col_i: usize) -> &mut [u8] {
        let col_offset = self.metadata.col_offsets[col_i];
        let col_size = self.header.col_sizes[col_i];
        let offset = col_offset + row_i * col_size;
        unsafe { slice::from_raw_parts_mut(self.data.offset(offset as isize), col_size) }
    }

    fn get_row_col(&self, row_i: usize, col_i: usize) -> &[u8] {
        &self.get_row_col_mut(row_i, col_i)[..]
    }

    fn get_row_mut(&self, row_i: usize) -> impl Iterator<Item = &mut [u8]> {
        (0..self.metadata.n_cols)
            .map(move |col_i| self.get_row_col_mut(row_i, col_i))
    }

    fn get_col_mut(&self, col_i: usize) -> impl Iterator<Item = &mut [u8]> {
        let col_offset = self.metadata.col_offsets[col_i];
        let col_size = self.header.col_sizes[col_i];
        (col_offset..)
            .step_by(col_size)
            .take(self.metadata.row_cap)
            .map(move |offset| unsafe {
                slice::from_raw_parts_mut(self.data.offset(offset as isize), col_size)
            })
    }

    fn get_rows_with_mask_mut(
        &self,
        mask: RowMask,
    ) -> impl Iterator<Item = (usize, impl Iterator<Item = &mut [u8]>)> {
        (0..self.metadata.row_cap)
            .zip(mask.bits.into_iter())
            .filter(|&(_, bit)| bit)
            .map(move |(row_i, _)| (row_i, self.get_row_mut(row_i)))
    }

    fn get_flag_for_row(&self, flag_i: usize, row_i: usize) -> bool {
        let buf = self.get_row_col_mut(row_i, self.metadata.n_cols);
        self.metadata.bits_type.get(flag_i, buf)
    }

    fn set_flag_for_row(&self, flag_i: usize, row_i: usize, val: bool) {
        let buf = self.get_row_col_mut(row_i, self.metadata.n_cols);
        self.metadata.bits_type.set(flag_i, val, buf)
    }

    fn set_valid_flag_for_row(&self, row_i: usize, val: bool) {
        self.set_flag_for_row(self.header.n_flags, row_i, val);
    }

    fn set_ready_flag_for_row(&self, row_i: usize, val: bool) {
        self.set_flag_for_row(self.header.n_flags + 1, row_i, val);
    }

    fn get_flag_for_rows<'a>(&'a self, flag_i: usize) -> impl Iterator<Item = bool> + 'a {
        (0..self.metadata.row_cap)
            .map(move |row_i| self.get_flag_for_row(flag_i, row_i))
    }

    fn set_flag_for_rows(&self, flag_i: usize, val: bool) {
        for row_i in 0..self.metadata.row_cap {
            self.set_flag_for_row(flag_i, row_i, val)
        }
    }

    fn get_valid_flag_for_rows<'a>(&'a self) -> impl Iterator<Item = bool> + 'a {
        self.get_flag_for_rows(self.header.n_flags)
    }

    fn get_ready_flag_for_rows<'a>(&'a self) -> impl Iterator<Item = bool> + 'a {
        self.get_flag_for_rows(self.header.n_flags + 1)
    }

    fn set_valid_flag_for_rows(&self, val: bool) {
        self.set_flag_for_rows(self.header.n_flags, val);
    }

    fn set_ready_flag_for_rows(&self, val: bool) {
        self.set_flag_for_rows(self.header.n_flags + 1, val);
    }
}

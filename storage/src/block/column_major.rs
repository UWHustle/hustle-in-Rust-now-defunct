use std::io::Cursor;
use std::iter::{FromIterator, Zip};
use std::ops::Range;
use std::slice;
use std::sync::Mutex;

use bit_vec::BitVec;
use memmap::MmapMut;

use hustle_types::{Bits, HustleType};
use byteorder::{WriteBytesExt, ReadBytesExt, LittleEndian};

pub struct RowMask {
    bits: BitVec
}

impl RowMask {
    pub fn and(&mut self, other: &Self) {
        self.bits.intersect(&other.bits);
    }

    pub fn or(&mut self, other: &Self) {
        self.bits.union(&other.bits);
    }
}

struct Header {
    col_sizes: Vec<usize>,
    n_flags: usize,
}

struct Metadata {
    n_rows: Mutex<usize>,
    n_cols: usize,
    row_cap: usize,
    col_indices: Vec<usize>,
    col_offsets: Vec<usize>,
    bits_type: Bits,
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

        let data_offset = {
            let mut cursor = Cursor::new(mmap.as_mut());

            cursor.write_u64::<LittleEndian>(col_sizes.len() as u64).unwrap();
            for &col_size in &col_sizes {
                cursor.write_u64::<LittleEndian>(col_size as u64).unwrap();
            }
            cursor.write_u64::<LittleEndian>(n_flags as u64).unwrap();

            cursor.position() as usize
        };

        let header = Header { col_sizes, n_flags };

        let block = Self::with_header(header, data_offset, mmap);

        block.delete_rows();

        block
    }

    pub fn from_buf(mut mmap: MmapMut) -> Self {
        let (header, data_offset) = {
            let mut cursor = Cursor::new(mmap.as_mut());

            let n_cols = cursor.read_u64::<LittleEndian>().unwrap() as usize;
            let col_sizes = (0..n_cols)
                .map(|_| cursor.read_u64::<LittleEndian>().unwrap() as usize)
                .collect::<Vec<_>>();
            let n_flags = cursor.read_u64::<LittleEndian>().unwrap() as usize;

            let header = Header { col_sizes, n_flags };
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

        let col_indices = (0..n_cols).collect();

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
            col_indices,
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

    pub fn n_cols(&self) -> usize {
        self.metadata.n_cols
    }

    pub fn is_full(&self) -> bool {
        *self.metadata.n_rows.lock().unwrap() == self.metadata.row_cap
    }

    pub fn get_row_col(&self, row_i: usize, col_i: usize) -> Option<&[u8]> {
        if self.get_valid_flag_for_row(row_i)
            && self.get_ready_flag_for_row(row_i)
            && col_i < self.n_cols()
        {
            Some(self.get_row_col_unchecked(row_i, col_i))
        } else {
            None
        }
    }

    pub fn row_ids(&self) -> RowIdIter {
        RowIdIter::new(self)
    }

    pub fn rows(&self) -> RowsIter {
        self.project(&self.metadata.col_indices)
    }

    pub fn project<'a>(&'a self, cols: &'a [usize]) -> RowsIter {
        RowsIter::new(cols, self)
    }

    pub fn rows_with_mask<'a>(&'a self, mask: &'a RowMask) -> MaskedRowsIter {
        self.project_with_mask(&self.metadata.col_indices, mask)
    }

    pub fn project_with_mask<'a>(&'a self, cols: &'a [usize], mask: &'a RowMask) -> MaskedRowsIter {
        MaskedRowsIter::new(mask, cols, self)
    }

    pub fn insert_row<'a>(&self, row: impl Iterator<Item = &'a [u8]>) {
        // Acquire a lock on the row counter so no other thread can write to the same row.
        let mut n_rows = self.metadata.n_rows.lock().unwrap();

        // Find the index of the first row with the state (!valid, ready).
        let (row_i, _) = self.get_valid_flag_for_rows()
            .zip(self.get_ready_flag_for_rows())
            .enumerate()
            .find(|&(_, (valid, ready))| !valid && ready)
            .expect("Cannot insert a row into a full block");

        // Increment the number of rows.
        *n_rows += 1;

        // Set the row state to (valid, ready).
        self.set_valid_flag_for_row(row_i, true);

        // Write the data to the row.
        for (dst_buf, src_buf) in self.get_row_unchecked_mut(row_i).zip(row) {
            dst_buf.copy_from_slice(src_buf);
        }
    }

    pub fn insert_rows<'a>(&self, rows: &mut impl Iterator<Item = impl Iterator<Item = &'a [u8]>>) {
        // Acquire a lock on the row counter so no other thread can write to the same rows.
        let mut n_rows = self.metadata.n_rows.lock().unwrap();

        // Build a mask where the "on" bits represent the indices of the rows with state
        // (!valid, ready).
        let bits = BitVec::from_iter(
            self.get_valid_flag_for_rows()
                .zip(self.get_ready_flag_for_rows())
                .map(|(valid, ready)| !valid && ready)
        );
        let mask = RowMask { bits };

        // Iterate over the rows with this mask.
        for ((row_i, dst_row), src_row) in self.get_rows_with_mask_mut(mask).zip(rows) {
            // Increment the number of rows.
            *n_rows += 1;

            // Set the row state to (valid, ready).
            self.set_valid_flag_for_row(row_i, true);

            // Write the data to the row.
            for (dst_buf, src_buf) in dst_row.zip(src_row) {
                dst_buf.copy_from_slice(src_buf);
            }
        }
    }

    pub fn delete_rows(&self) {
        self.set_valid_flag_for_rows(false);
        self.set_ready_flag_for_rows(true);
        *self.metadata.n_rows.lock().unwrap() = 0;
    }

    pub fn delete_rows_with_mask(&self, mask: &RowMask) {
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

    pub fn update_col_with_mask(&self, col_i: usize, value: &[u8], mask: &RowMask) {
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

    pub fn filter_cols(
        &self,
        l_col_i: usize,
        r_col_i: usize,
        f: impl Fn(&[u8], &[u8]) -> bool
    ) -> RowMask {
        let bits = BitVec::from_iter(
            self.get_valid_flag_for_rows()
                .zip(self.get_ready_flag_for_rows())
                .zip(self.get_col_mut(l_col_i))
                .zip(self.get_col_mut(r_col_i))
                .map(|(((valid, ready), l_buf), r_buf)|
                    valid && ready && f(l_buf, r_buf)
                )
        );

        RowMask { bits }
    }

    fn get_row_col_unchecked_mut(&self, row_i: usize, col_i: usize) -> &mut [u8] {
        let col_offset = self.metadata.col_offsets[col_i];
        let col_size = self.header.col_sizes[col_i];
        let offset = col_offset + row_i * col_size;
        unsafe { slice::from_raw_parts_mut(self.data.offset(offset as isize), col_size) }
    }

    fn get_row_col_unchecked(&self, row_i: usize, col_i: usize) -> &[u8] {
        &self.get_row_col_unchecked_mut(row_i, col_i)[..]
    }

    fn get_row_unchecked_mut(&self, row_i: usize) -> impl Iterator<Item = &mut [u8]> {
        (0..self.metadata.n_cols)
            .map(move |col_i| self.get_row_col_unchecked_mut(row_i, col_i))
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
            .map(move |(row_i, _)| (row_i, self.get_row_unchecked_mut(row_i)))
    }

    fn get_flag_for_row(&self, flag_i: usize, row_i: usize) -> bool {
        let buf = self.get_row_col_unchecked_mut(row_i, self.metadata.n_cols);
        self.metadata.bits_type.get(flag_i, buf)
    }

    fn set_flag_for_row(&self, flag_i: usize, row_i: usize, val: bool) {
        let buf = self.get_row_col_unchecked_mut(row_i, self.metadata.n_cols);
        self.metadata.bits_type.set(flag_i, val, buf)
    }

    fn get_valid_flag_for_row(&self, row_i: usize) -> bool {
        self.get_flag_for_row(self.header.n_flags, row_i)
    }

    fn get_ready_flag_for_row(&self, row_i: usize) -> bool {
        self.get_flag_for_row(self.header.n_flags + 1, row_i)
    }

    fn set_valid_flag_for_row(&self, row_i: usize, val: bool) {
        self.set_flag_for_row(self.header.n_flags, row_i, val);
    }

    fn set_ready_flag_for_row(&self, row_i: usize, val: bool) {
        self.set_flag_for_row(self.header.n_flags + 1, row_i, val);
    }

    fn get_flag_for_rows(&self, flag_i: usize) -> FlagIter {
        FlagIter::new(flag_i, self)
    }

    fn set_flag_for_rows(&self, flag_i: usize, val: bool) {
        for row_i in 0..self.metadata.row_cap {
            self.set_flag_for_row(flag_i, row_i, val)
        }
    }

    fn get_valid_flag_for_rows(&self) -> FlagIter {
        self.get_flag_for_rows(self.header.n_flags)
    }

    fn get_ready_flag_for_rows(&self) -> FlagIter {
        self.get_flag_for_rows(self.header.n_flags + 1)
    }

    fn set_valid_flag_for_rows(&self, val: bool) {
        self.set_flag_for_rows(self.header.n_flags, val);
    }

    fn set_ready_flag_for_rows(&self, val: bool) {
        self.set_flag_for_rows(self.header.n_flags + 1, val);
    }
}

#[derive(Clone)]
pub struct FlagIter<'a> {
    flag_i: usize,
    iter: Range<usize>,
    block: &'a ColumnMajorBlock,
}

impl<'a> FlagIter<'a> {
    fn new(flag_i: usize, block: &'a ColumnMajorBlock) -> Self {
        let iter = 0..block.metadata.row_cap;
        FlagIter {
            flag_i,
            iter,
            block,
        }
    }
}

impl<'a> Iterator for FlagIter<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
            .map(|row_i| self.block.get_flag_for_row(self.flag_i, row_i))
    }
}

#[derive(Clone)]
pub struct RowIdIter<'a> {
    iter: Zip<Zip<Range<usize>, FlagIter<'a>>, FlagIter<'a>>,
}

impl<'a> RowIdIter<'a> {
    fn new(block: &'a ColumnMajorBlock) -> Self {
        let iter = (0..block.metadata.row_cap)
            .zip(block.get_valid_flag_for_rows())
            .zip(block.get_ready_flag_for_rows());
        RowIdIter {
            iter,
        }
    }
}

impl<'a> Iterator for RowIdIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(((row_i, valid), ready)) = self.iter.next() {
                if valid && ready {
                    break Some(row_i)
                }
            } else {
                break None
            }
        }
    }
}

#[derive(Clone)]
pub struct RowIter<'a> {
    row_i: usize,
    iter: slice::Iter<'a, usize>,
    block: &'a ColumnMajorBlock,
}

impl<'a> RowIter<'a> {
    fn new(row_i: usize, cols: &'a [usize], block: &'a ColumnMajorBlock) -> Self {
        let iter = cols.iter();
        RowIter {
            row_i,
            iter,
            block,
        }
    }
}

impl<'a> Iterator for RowIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
            .map(|&col_i| self.block.get_row_col_unchecked(self.row_i, col_i))
    }
}

#[derive(Clone)]
pub struct RowsIter<'a> {
    cols: &'a [usize],
    iter: RowIdIter<'a>,
    block: &'a ColumnMajorBlock,
}

impl<'a> RowsIter<'a> {
    fn new(cols: &'a [usize], block: &'a ColumnMajorBlock) -> Self {
        let iter = block.row_ids();
        RowsIter {
            cols,
            iter,
            block,
        }
    }
}

impl<'a> Iterator for RowsIter<'a> {
    type Item = RowIter<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
            .map(|row_i| RowIter::new(row_i, self.cols, self.block))
    }
}

pub struct MaskedRowsIter<'a> {
    cols: &'a [usize],
    iter: Zip<Range<usize>, bit_vec::Iter<'a>>,
    block: &'a ColumnMajorBlock,
}

impl<'a> MaskedRowsIter<'a> {
    fn new(mask: &'a RowMask, cols: &'a [usize], block: &'a ColumnMajorBlock) -> Self {
        let iter = (0..block.metadata.row_cap)
            .zip(&mask.bits);
        MaskedRowsIter {
            cols,
            iter,
            block,
        }
    }
}

impl<'a> Iterator for MaskedRowsIter<'a> {
    type Item = RowIter<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((row_i, bit)) = self.iter.next() {
                if bit {
                    break Some(RowIter::new(row_i, self.cols, self.block))
                }
            } else {
                break None
            }
        }
    }
}

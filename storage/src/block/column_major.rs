use std::slice;
use std::io::Cursor;
use std::iter::{Enumerate, FromIterator, StepBy, Take, Zip};
use std::ops::{Range, RangeFrom};

use bit_vec::BitVec;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use memmap::MmapMut;

use hustle_types::{Bits, HustleType};

/// A mask that can be used to specify which rows of the block should be included in a result. The
/// underlying information is a vector of bits. The `RowMask` can only be constructed by calling the
/// functions `filter_col` or `filter_cols` on a `ColumnMajorBlock`.
pub struct RowMask {
    bits: BitVec
}

impl RowMask {
    pub fn intersect(&mut self, other: &Self) {
        self.bits.intersect(&other.bits);
    }

    pub fn union(&mut self, other: &Self) {
        self.bits.union(&other.bits);
    }
}

struct Header {
    col_sizes: Vec<usize>,
    n_flags: usize,
}

struct Metadata {
    n_cols: usize,
    row_cap: usize,
    col_indices: Vec<usize>,
    col_offsets: Vec<usize>,
    bits_type: Bits,
}

/// A block that organizes tabular data in column-major format. A `ColumnMajorBlock` typically
/// represents a horizontal partition of a table in the system.
pub struct ColumnMajorBlock {
    header: Header,
    metadata: Metadata,
    data: *mut u8,
    _mmap: MmapMut,
}

impl ColumnMajorBlock {
    /// Returns a new `ColumnMajorBlock` with the specified `col_sizes` and number of bit flags,
    /// using `mmap` as the backing memory buffer. This should only be called with newly constructed
    /// `MmapMut` buffers.
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

    /// Attempts to construct a `ColumnMajorBlock` from an existing `mmap` memory buffer.
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

    /// Returns the number of columns in the `ColumnMajorBlock`.
    pub fn n_cols(&self) -> usize {
        self.metadata.n_cols
    }

    /// Returns an iterator over the buffers in the row with the specified `row_i`, if the row is
    /// valid and in bounds.
    pub fn get_row(&self, row_i: usize) -> Option<RowIter> {
        if row_i < self.metadata.row_cap
            && self.get_valid_flag_for_row(row_i)
            && self.get_ready_flag_for_row(row_i)
        {
            Some(RowIter::new(row_i, &self.metadata.col_indices, self))
        } else {
            None
        }
    }

    /// Returns an iterator over tuples containing the row ID and buffer in the column with the
    /// specified `col_i`, if the column is valid.
    pub fn get_col(&self, col_i: usize) -> Option<ColIter> {
        if col_i < self.n_cols() {
            Some(ColIter::new(col_i, self))
        } else {
            None
        }
    }

    /// Returns the buffer at the specified `row_i` and `col_i`, if the row is valid and the row and
    /// column are in bounds of the `ColumnMajorBlock`.
    pub fn get_row_col(&self, row_i: usize, col_i: usize) -> Option<&[u8]> {
        if row_i < self.metadata.row_cap
            && col_i < self.n_cols()
            && self.get_valid_flag_for_row(row_i)
            && self.get_ready_flag_for_row(row_i)
        {
            Some(self.get_row_col_unchecked(row_i, col_i))
        } else {
            None
        }
    }

    /// Returns an iterator over the valid row IDs in the `ColumnMajorBlock`.
    pub fn row_ids(&self) -> RowIdIter {
        RowIdIter::new(self)
    }

    /// Returns a row-major iterator over the rows of the `ColumnMajorBlock`. The item of the
    /// returned iterator is itself an iterator over the current row.
    pub fn rows(&self) -> RowsIter {
        self.project(&self.metadata.col_indices)
    }

    /// Returns a row-major iterator over the rows of the `ColumnMajorBlock` which only includes the
    /// specified columns.
    pub fn project<'a>(&'a self, cols: &'a [usize]) -> RowsIter {
        RowsIter::new(cols, self)
    }

    /// Returns a row-major iterator over the rows of the `ColumnMajorBlock` which only includes the
    /// rows specified by the `mask`.
    pub fn rows_with_mask<'a>(&'a self, mask: &'a RowMask) -> MaskedRowsIter {
        self.project_with_mask(&self.metadata.col_indices, mask)
    }

    /// Returns a row-major iterator over the rows of the `ColumnMajorBlock` which only includes the
    /// specified columns and the rows specified by the `mask`.
    pub fn project_with_mask<'a>(&'a self, cols: &'a [usize], mask: &'a RowMask) -> MaskedRowsIter {
        MaskedRowsIter::new(mask, cols, self)
    }

    /// Inserts a new row into the `ColumnMajorBlock` at the earliest available location. A `panic!`
    /// will occur if the block is full or the size of each slice does not equal the size of its
    /// corresponding column.
    pub fn insert_row<'a>(&self, row: impl Iterator<Item = &'a [u8]>) {
        // Find the index of the first row with the state (!valid, ready).
        let (row_i, _) = self.get_valid_flag_for_rows()
            .zip(self.get_ready_flag_for_rows())
            .enumerate()
            .find(|&(_, (valid, ready))| !valid && ready)
            .expect("Cannot insert a row into a full block");

        // Set the row state to (valid, ready).
        self.set_valid_flag_for_row(row_i, true);

        // Write the data to the row.
        for (dst_buf, src_buf) in self.get_row_unchecked_mut(row_i).zip(row) {
            dst_buf.copy_from_slice(src_buf);
        }
    }

    /// Inserts rows into the `ColumnMajorBlock`, advancing the `rows` iterator until it is
    /// exhausted or the block is full. A `panic!` will occur if the size of each slice does not
    /// equal the size of its corresponding column.
    pub fn insert_rows<'a>(&self, rows: &mut impl Iterator<Item = impl Iterator<Item = &'a [u8]>>) {
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

            // Set the row state to (valid, ready).
            self.set_valid_flag_for_row(row_i, true);

            // Write the data to the row.
            for (dst_buf, src_buf) in dst_row.zip(src_row) {
                dst_buf.copy_from_slice(src_buf);
            }
        }
    }

    /// Deletes all rows in the `ColumnMajorBlock`.
    pub fn delete_rows(&self) {
        self.set_valid_flag_for_rows(false);
        self.set_ready_flag_for_rows(true);
    }

    /// Deletes the rows in the `ColumnMajorBlock` specified by the `mask`.
    pub fn delete_rows_with_mask(&self, mask: &RowMask) {
        let row_is = mask.bits.iter()
            .enumerate()
            .filter(|&(_, bit)| bit);

        for (row_i, _) in row_is {
            self.set_valid_flag_for_row(row_i, false);
            self.set_ready_flag_for_row(row_i, true);
        }
    }

    /// Sets the value of the column specified by `col_i` to `value`.
    pub fn update_col(&self, col_i: usize, value: &[u8]) {
        for buf in ColIterUncheckedMut::new(col_i, self) {
            buf.copy_from_slice(value);
        }
    }

    /// Sets the value of the column specified by `col_i` to `value`, only updating the rows
    /// specified by `mask`.
    pub fn update_col_with_mask(&self, col_i: usize, value: &[u8], mask: &RowMask) {
        let bufs = ColIterUncheckedMut::new(col_i, self)
            .zip(mask.bits.iter())
            .filter(|&(_, bit)| bit);

        for (buf, _) in bufs {
            buf.copy_from_slice(value);
        }
    }

    /// Returns a new `RowMask` by applying the predicate `f` on each buffer in the column specified
    /// by `col_i`.
    pub fn filter_col(&self, col_i: usize, f: impl Fn(&[u8]) -> bool) -> RowMask {
        let bits = BitVec::from_iter(
            self.get_valid_flag_for_rows()
                .zip(self.get_ready_flag_for_rows())
                .zip(ColIterUncheckedMut::new(col_i, self))
                .map(|((valid, ready), buf)| valid && ready && f(buf))
        );

        RowMask { bits }
    }

    /// Returns a new `RowMask` by applying the predicate `f` on each buffer in the columns
    /// specified by `l_col_i` and `r_col_i`. The rows of the left and right columns are iterated
    /// in parallel.
    pub fn filter_cols(
        &self,
        l_col_i: usize,
        r_col_i: usize,
        f: impl Fn(&[u8], &[u8]) -> bool
    ) -> RowMask {
        let bits = BitVec::from_iter(
            self.get_valid_flag_for_rows()
                .zip(self.get_ready_flag_for_rows())
                .zip(ColIterUncheckedMut::new(l_col_i, self))
                .zip(ColIterUncheckedMut::new(r_col_i, self))
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

struct ColIterUncheckedMut<'a> {
    col_size: usize,
    iter: Take<StepBy<RangeFrom<usize>>>,
    block: &'a ColumnMajorBlock,
}

impl<'a> ColIterUncheckedMut<'a> {
    fn new(col_i: usize, block: &'a ColumnMajorBlock) -> Self {
        let col_offset = block.metadata.col_offsets[col_i];
        let col_size = block.header.col_sizes[col_i];
        let iter = (col_offset..)
            .step_by(col_size)
            .take(block.metadata.row_cap);

        ColIterUncheckedMut {
            col_size,
            iter,
            block,
        }
    }
}

impl<'a> Iterator for ColIterUncheckedMut<'a> {
    type Item = &'a mut [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
            .map(|offset| unsafe {
                slice::from_raw_parts_mut(self.block.data.offset(offset as isize), self.col_size)
            })
    }
}

pub struct ColIter<'a> {
    iter: Enumerate<Zip<Zip<ColIterUncheckedMut<'a>, FlagIter<'a>>, FlagIter<'a>>>,
}

impl<'a> ColIter<'a> {
    fn new(col_i: usize, block: &'a ColumnMajorBlock) -> Self {
        let iter = ColIterUncheckedMut::new(col_i, block)
            .zip(block.get_valid_flag_for_rows())
            .zip(block.get_ready_flag_for_rows())
            .enumerate();

        ColIter {
            iter,
        }
    }
}

impl<'a> Iterator for ColIter<'a> {
    type Item = (usize, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((row_i, ((buf, valid), ready))) = self.iter.next() {
                if valid && ready {
                    break Some((row_i, &buf[..]))
                }
            } else {
                break None
            }
        }
    }
}

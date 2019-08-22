use std::io::Cursor;
use std::slice;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use memmap::MmapMut;

pub struct ColumnMajorBlock {
    col_sizes: Vec<usize>,
    col_offsets: Vec<usize>,
    n_rows: usize,
    data: *mut u8,
    _mmap: MmapMut,
}

impl ColumnMajorBlock {
    pub fn new(col_sizes: Vec<usize>, mut mmap: MmapMut) -> Self {
        assert!(col_sizes.len() > 0, "Cannot create a block with no columns");

        let data_offset = {
            let mut cursor = Cursor::new(mmap.as_mut());
            cursor.write_u64::<BigEndian>(col_sizes.len() as u64).unwrap();

            for &col_size in &col_sizes {
                assert!(col_size > 0, "Column size must be a positive integer");
                cursor.write_u64::<BigEndian>(col_size as u64).unwrap();
            }

            cursor.position() as usize
        };

        Self::with_col_sizes(col_sizes, data_offset, mmap)
    }

    pub fn from_buf(mut mmap: MmapMut) -> Self {
        let (col_sizes, data_offset) = {
            let mut cursor = Cursor::new(mmap.as_mut());
            let n_cols = cursor.read_u64::<BigEndian>().unwrap() as usize;

            let col_sizes = (0..n_cols)
                .map(|_| cursor.read_u64::<BigEndian>().unwrap() as usize)
                .collect::<Vec<usize>>();

            (col_sizes, cursor.position() as usize)
        };

        Self::with_col_sizes(col_sizes, data_offset, mmap)
    }

    fn with_col_sizes(col_sizes: Vec<usize>, data_offset: usize, mut mmap: MmapMut) -> Self {
        let (data, data_len) = {
            let data = &mut mmap[data_offset..];
            (data.as_mut_ptr(), data.len())
        };

        let n_rows = data_len / col_sizes.iter().sum::<usize>();

        let col_offsets = col_sizes.iter()
            .scan(0, |state, &col_size| {
                let col_offset = *state;
                *state = col_offset + n_rows * col_size;
                Some(col_offset)
            })
            .collect::<Vec<usize>>();

        ColumnMajorBlock {
            col_sizes,
            col_offsets,
            n_rows,
            data,
            _mmap: mmap,
        }
    }

    pub fn get_col_sizes(&self) -> &[usize] {
        &self.col_sizes
    }

    pub fn get_rows<'a>(&'a self)
        -> impl Iterator<Item = impl Iterator<Item = &'a mut [u8]> + 'a> + 'a
    {
        (0..self.n_rows)
            .map(move |row| self.get_row(row))
    }

    pub fn get_cols<'a>(&'a self)
        -> impl Iterator<Item = impl Iterator<Item = &'a mut [u8]> + 'a> + 'a
    {
        (0..self.col_sizes.len())
            .map(move |row| self.get_col(row))
    }

    pub fn get_row<'a>(&'a self, row: usize) -> impl Iterator<Item = &'a mut [u8]> + 'a {
        self.assert_row_in_bounds(row);

        self.col_offsets.iter()
            .zip(&self.col_sizes)
            .map(move |(&col_offset, &col_size)| {
                let offset = col_offset + row * col_size;
                unsafe { slice::from_raw_parts_mut(self.data.offset(offset as isize), col_size) }
            })
    }

    pub fn get_col<'a>(&'a self, col: usize) -> impl Iterator<Item = &'a mut [u8]> + 'a {
        self.assert_col_in_bounds(col);

        let col_size = self.col_sizes[col];
        (self.col_offsets[col]..)
            .step_by(col_size)
            .take(self.n_rows)
            .map(move |offset| {
                unsafe { slice::from_raw_parts_mut(self.data.offset(offset as isize), col_size) }
            })
    }

    pub fn get_row_col(&self, row: usize, col: usize) -> &mut [u8] {
        self.assert_row_in_bounds(row);
        self.assert_col_in_bounds(col);

        let col_size = self.col_sizes[col];
        let offset = self.col_offsets[col] + row * col_size;
        unsafe { slice::from_raw_parts_mut(self.data.offset(offset as isize), col_size) }
    }

    fn assert_row_in_bounds(&self, row: usize) {
        assert!(
            row < self.n_rows,
            "Row {} out of bounds for block with {} rows",
            row,
            self.n_rows,
        );
    }

    fn assert_col_in_bounds(&self, col: usize) {
        assert!(
            col < self.col_sizes.len(),
            "Column {} out of bounds for block with {} columns",
            col,
            self.col_sizes.len(),
        );
    }
}

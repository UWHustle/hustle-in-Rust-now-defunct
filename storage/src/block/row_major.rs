use std::io::{Cursor, Write};

use memmap::MmapMut;

use block::{BitMap, Header, RawSlice};

/// A `RowMajorBlock` is a horizontal partition of a `PhysicalRelation` in row-major order.
pub struct RowMajorBlock {
    header: Header,
    valid: BitMap,
    ready: BitMap,
    data: RawSlice,
    col_bounds: Vec<usize>,
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
        // Reserve space for the two bitmaps and the raw data.
        let bitmap_size = header.get_bitmap_size();

        let valid_start = header.size();
        let ready_start = valid_start + bitmap_size;
        let data_start = ready_start + bitmap_size;

        let valid = BitMap::new(&mut mmap[valid_start..ready_start]);
        let ready = BitMap::new(&mut mmap[ready_start..data_start]);
        let data = RawSlice::new(&mut mmap[data_start..]);

        // Pre-calculate the bounds for each column.
        let mut col_bounds = header.get_schema().iter()
            .scan(0, |state, &col_size| {
                let offset = *state;
                *state += col_size;
                Some(offset)
            })
            .collect::<Vec<usize>>();
        col_bounds.push(header.get_row_size());

        RowMajorBlock {
            header,
            valid,
            ready,
            data,
            col_bounds,
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

    fn project<'a>(
        &'a self,
        cols: &'a [usize]
    ) -> impl Iterator<Item = impl Iterator<Item = &'a [u8]> + 'a> + 'a {
        self.valid.iter()
            .zip(self.ready.iter())
            .zip((0..).step_by(self.get_row_size()))
            .filter(|((valid, ready), _)| *valid && *ready)
            .map(move |(_, offset)| {
                cols.iter().map(move |&col| {
                    let start = offset + self.col_bounds[col];
                    let end = offset + self.col_bounds[col + 1];
                    &self.data.as_slice()[start..end]
                })
            })
    }

    /// Inserts a new row into the block. The row is inserted into the first available slot.
    pub fn insert(&self, row: &[&[u8]]) {
        let data = self.data.as_slice();
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
            assert_ne!(n_rows, self.get_row_capacity(), "Block is already at capacity");
            n_rows_guard.set(n_rows + 1);

            // Find the position in the block to insert the row.
            self.row_offsets()
                .find(|(_, _, valid, ready)| !*valid && *ready)
                .unwrap()
                .1
        };

        // Write the new row to the block.
        let mut cursor = Cursor::new(&mut data[offset..]);
        for &col in row {
            cursor.write(col).unwrap();
        }
    }

    /// For each row in the block, deletes the row if `filter` called on that row returns true.
    pub fn delete<F>(&mut self, filter: F) where F: Fn(&[&[u8]]) -> bool {
        let data = self.data.as_slice();
        let schema = self.get_schema();
        let mut row_buf = vec![&[] as &[u8]; schema.len()];

        let rows = (0..self.get_row_capacity())
            .zip((0..).step_by(self.get_row_size()));

        for (row_i, offset) in rows {
            if self.valid.get_unchecked(row_i) && self.ready.get_unchecked(row_i) {
                for (col, col_size) in schema.iter().enumerate() {
                    let start = offset + self.col_bounds[col];
                    let end = start + *col_size;
                    row_buf[col] = &data[start..end];
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



//
//pub struct RowIter<'a, I: Iterator<Item = &'a [u8]>> {
//    value_iter: I,
//}
//
//impl<'a, I: Iterator<Item = &'a [u8]>> Iterator for RowIter<'a, I> {
//    type Item = &'a [u8];
//
//    fn next(&mut self) -> Option<Self::Item> {
//        unimplemented!()
//    }
//}
//
//pub struct ProjectIter<'a, I: Iterator<Item = usize>> {
//    offset_iter: I,
//    data: &'a [u8],
//    col_bounds: Vec<(usize, usize)>,
//}
//
//impl<'a, I: Iterator<Item = usize>> ProjectIter<'a, I> {
//    fn new(offset_iter: I, data: &'a [u8], col_bounds: Vec<(usize, usize)>) -> Self {
//        ProjectIter {
//            offset_iter,
//            data,
//            col_bounds,
//        }
//    }
//}
//
//impl<'a, I: Iterator<Item = usize>> Iterator for ProjectIter<'a, I> {
//    type Item = RowIter<'a, Map<slice::Iter<'a, (usize, usize)>, FnMut(&(usize, usize)) -> &'a [u8]>>;
//
//    fn next(&mut self) -> Option<Self::Item> {
//        self.offset_iter.next().map(|offset| {
//            let value_iter = self.col_bounds.iter().map(|&(start, end)|
//                &self.data[start..end]
//            );
//            RowIter { value_iter }
//        })
//    }
//}
//








//pub struct RowIter<'a, I: Iterator<Item = &'a [u8]>> {
//    value_iter: I,
//}
//
//impl<'a, I: Iterator<Item = &'a [u8]>> Iterator for RowIter<'a, I> {
//    type Item = &'a [u8];
//
//    fn next(&mut self) -> Option<Self::Item> {
//        unimplemented!()
//    }
//}
//
//pub struct ProjectIter<'a, I1: Iterator<Item = usize>, I2: Iterator<Item = &'a [u8]>> {
//    offset_iter: I1,
//    data: &'a [u8],
//    col_bounds: Vec<(usize, usize)>,
//    phantom: PhantomData<I2>,
//}
//
//impl<'a, I1: Iterator<Item = usize>, I2: Iterator<Item = &'a [u8]>> ProjectIter<'a, I1, I2> {
//    fn new(offset_iter: I1, data: &'a [u8], col_bounds: Vec<(usize, usize)>) -> Self {
//        ProjectIter {
//            offset_iter,
//            data,
//            col_bounds,
//            phantom: PhantomData
//        }
//    }
//}
//
//impl<'a, I1: Iterator<Item = usize>, I2: Iterator<Item = &'a [u8]>> Iterator for ProjectIter<'a, I1, I2> {
//    type Item = RowIter<'a, I2>;
//
//    fn next(&mut self) -> Option<Self::Item> {
//        self.offset_iter.next().map(|offset| {
//            let value_iter = self.col_bounds.iter().map(|&(start, end)|
//                &self.data[start..end]
//            );
//            RowIter { value_iter }
//        })
//    }
//}

//fn project2<'a>(&'a self, cols: &'a [usize]) -> impl Iterator+ 'a {
//        self.valid.iter()
//            .zip(self.ready.iter())
//            .zip((0..).step_by(self.get_row_size()))
//            .filter(|((valid, ready), _)| *valid && *ready)
//            .map(move |(_, offset)|
//                cols.iter().map(move |&col| {
//                    let start = offset + self.col_bounds[col];
//                    let end = offset + self.col_bounds[col + 1];
//                    &self.data.as_slice()[start..end]
//                })
//            )
//    }

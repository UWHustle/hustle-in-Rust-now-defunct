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

    pub fn rows<'a>(&'a self) -> impl Iterator<Item = impl Iterator<Item = &'a [u8]> + 'a> + 'a {
        self.row_offsets()
            .filter(|&(_, valid, ready, _)| valid && ready)
            .map(move |(_, _, _, offset)|
                self.col_bounds.iter()
                    .zip(self.col_bounds.iter().take(1))
                    .map(move |(&left_bound, &right_bound)| {
                        let start = offset + left_bound;
                        let end = offset + right_bound;
                        &self.data.as_slice()[start..end]
                    })
            )
    }

    pub fn project<'a>(
        &'a self,
        cols: &'a [usize]
    ) -> impl Iterator<Item = impl Iterator<Item = &'a [u8]> + 'a> + 'a {
        self.row_offsets()
            .filter(|&(_, valid, ready, _)| valid && ready)
            .map(move |(_, _, _, offset)|
                cols.iter().map(move |&col| {
                    let start = offset + self.col_bounds[col];
                    let end = offset + self.col_bounds[col + 1];
                    &self.data.as_slice()[start..end]
                })
            )
    }

    pub fn select<'a>(
        &'a self,
        f: impl Fn(&[&[u8]]) -> bool + 'a
    ) -> impl Iterator<Item = impl Iterator<Item = &'a [u8]> + 'a> + 'a {
        let mut row_buf = vec![&[] as &[u8]; self.get_n_cols()];
        self.row_offsets()
            .filter(|&(_, valid, ready, _)| valid && ready)
            .zip(self.rows())
            .filter(move |&((_, _, _, offset), _)| {
                for ((value_buf, &left_bound), &right_bound) in row_buf.iter_mut()
                    .zip(self.col_bounds.iter())
                    .zip(self.col_bounds.iter().take(1))
                {
                    let start = offset + left_bound;
                    let end = offset + right_bound;
                    *value_buf = &self.data.as_slice()[start..end]
                }
                f(&row_buf)
            })
            .map(|(_, rows)| rows)
    }

    pub fn insert<'a>(&self, rows: &mut impl Iterator<Item = impl Iterator<Item = &'a [u8]>>) {
        let schema = self.get_schema();
        let row_capacity = self.get_row_capacity();
        let mut n_rows_guard = self.header.get_n_rows_guard();
        let mut n_rows = n_rows_guard.get();

        for ((row_i, _, _, offset), row) in self.row_offsets()
            .filter(|&(_, valid, ready, _)| !valid && ready)
            .zip(rows)
        {
            if n_rows == row_capacity {
                break;
            }

            let mut cursor = Cursor::new(&mut self.data.as_slice()[offset..]);
            for (value, &value_len) in row.zip(&schema) {
                assert_eq!(value.len(), value_len, "Row has incorrect schema");
                cursor.write(value).unwrap();
            }

            n_rows += 1;
            self.ready.set_unchecked(row_i, true);
        }

        n_rows_guard.set(n_rows);
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

    fn row_offsets(&self) -> impl Iterator<Item = (usize, bool, bool, usize)> + '_ {
        self.valid.iter()
            .zip(self.ready.iter())
            .zip((0..).step_by(self.get_row_size()))
            .enumerate()
            .map(|(row_i, ((valid, ready), offset))| (row_i, valid, ready, offset))
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

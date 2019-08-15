use std::slice;

use memmap::MmapMut;

use block::Header;

pub struct ColumnMajorBlock {
    header: Header,
    data: *mut u8,
    data_len: usize,
    col_offsets: Vec<usize>,
    _mmap: MmapMut,
}

impl ColumnMajorBlock {
    pub fn new(schema: &[usize], mut mmap: MmapMut) -> Self {
        Self::with_header(Header::new(schema, &mut mmap), mmap)
    }

    pub fn from_buf(mut mmap: MmapMut) -> Self {
        Self::with_header(Header::from_buf(&mut mmap), mmap)
    }

    fn with_header(header: Header, mut mmap: MmapMut) -> Self {
        let header_len = header.len();
        let data = mmap[header_len..].as_mut_ptr();
        let data_len = mmap.len() - header_len;
        let col_offsets = unsafe { header.get_schema() }.iter()
            .scan(0, |state, &col_size| {
                let offset = *state;
                *state += col_size;
                Some(offset as usize)
            })
            .collect();

        ColumnMajorBlock {
            header,
            data,
            data_len,
            col_offsets,
            _mmap: mmap,
        }
    }

    pub fn get_n_rows(&self) -> usize {
        unsafe { self.header.get_n_rows() as usize }
    }

    pub fn get_n_cols(&self) -> usize {
        unsafe { self.header.get_n_cols() as usize }
    }

    pub fn get_schema(&self) -> &[usize] {
        unsafe { self.header.get_schema() }
    }

    pub fn get_row_col(&self, row: usize, col: usize) -> &[u8] {
        let size = self.get_schema()[col];
        let left = self.col_offsets[col] * (row * size);
        let right = left + size;
        &self.get_data()[left..right]
    }

    pub fn get_row<'a>(&'a self, row: usize) -> impl Iterator<Item = &'a [u8]> + 'a {
        let data = self.get_data();
        self.col_offsets.iter()
            .zip(self.get_schema())
            .map(move |(&col_offset, &size)| {
                let left = col_offset + (row * size);
                let right = left + size;
                &data[left..right]
            })
    }

    pub fn get_col<'a>(&'a self, col: usize) -> impl Iterator<Item = &'a [u8]> + 'a {
        let data = self.get_data();
        let col_offset = self.col_offsets[col];
        let size = self.get_schema()[col];
        (col_offset..).step_by(size)
            .zip((col_offset + size..).step_by(size))
            .take(self.get_n_rows())
            .map(move |(left, right)| &data[left..right])
    }

    pub fn get_rows<'a>(&'a self) -> impl Iterator<Item = impl Iterator<Item = &'a [u8]> + 'a> + 'a {
        (0..self.get_n_rows())
            .map(move |row| self.get_row(row))
    }

    pub fn get_cols<'a>(&'a self) -> impl Iterator<Item = impl Iterator<Item = &'a [u8]> + 'a> + 'a {
        (0..self.get_n_cols())
            .map(move |col| self.get_col(col))
    }

    fn get_data(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.data, self.data_len) }
    }
}

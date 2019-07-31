use std::{slice, ptr};
use block::{BLOCK_SIZE};
use std::ops::DerefMut;
use byteorder::{LittleEndian, ByteOrder, ReadBytesExt};
use std::mem::size_of;
use std::io::Cursor;
use std::convert::TryFrom;

type HeaderSize = usize;
type NRows = u64;
type NCols = u64;
type RowSize = u64;
type ColSize = u64;
type RowCapacity = u64;

#[derive(Clone)]
pub struct Header2<D> {
    buf: D,
    header_size_offset: usize,
    n_rows_offset: usize,
    n_cols_offset: usize,
    row_size_offset: usize,
    row_capacity_offset: usize,
    schema_offset: usize,
}

impl<D> Header2<D> where D: DerefMut<Target = [u8]> {
    pub fn new(schema: &[usize], buf: D) -> Self {
        // Blocks must have valid schema.
        assert!(!schema.is_empty(), "Cannot create a block with empty schema");
        assert!(
            schema.iter().all(|col_size| *col_size > 0),
            "Column size must be positive integer",
        );

        let n_cols = schema.len();
        let row_size: usize = schema.iter().sum();

        let remaining_block_size = BLOCK_SIZE
            - size_of::<NRows>()
            - size_of::<NCols>()
            - size_of::<RowSize>()
            - size_of::<RowCapacity>()
            - schema.len() * size_of::<ColSize>();

        // Calculate row capacity, accounting for two bits per row in the bitmap.
        let row_capacity = remaining_block_size * 8 / (row_size * 8 + 2);

        assert!(row_capacity > 0, "Row is too large to fit in a single block");

        let mut header = Self::from_slice(buf);
        header.set_n_rows(0);
        header.set_n_cols(n_cols as NCols);
        header.set_row_size(row_size as RowSize);
        header.set_row_capacity(row_capacity as RowCapacity);
        header.set_schema(schema);

        header
    }

    pub fn from_slice(buf: D) -> Self {
        let header_size_offset = 0;
        let n_rows_offset = header_size_offset + size_of::<HeaderSize>();
        let n_cols_offset = n_rows_offset + size_of::<NRows>();
        let row_size_offset = n_cols_offset + size_of::<NCols>();
        let row_capacity_offset = row_size_offset + size_of::<RowSize>();
        let schema_offset = row_capacity_offset + size_of::<RowCapacity>();
        let size = 0;

        let header = Header2 {
            buf,
            header_size_offset,
            n_rows_offset,
            n_cols_offset,
            row_size_offset,
            row_capacity_offset,
            schema_offset,
        };

        header
    }

    pub fn size(&self) -> usize {
        LittleEndian::read_uint(&self.buf[self.header_size_offset..], size_of::<HeaderSize>())
            as HeaderSize
    }

    pub fn get_n_rows(&self) -> NRows {
        LittleEndian::read_uint(&self.buf[self.n_rows_offset..], size_of::<NRows>())
    }

    pub fn get_n_cols(&self) -> NCols {
        LittleEndian::read_uint(&self.buf[self.n_cols_offset..], size_of::<NCols>())
    }

    pub fn get_row_size(&self) -> RowSize {
        LittleEndian::read_uint(&self.buf[self.row_size_offset..], size_of::<RowSize>())
    }

    pub fn get_row_capacity(&self) -> RowCapacity {
        LittleEndian::read_uint(&self.buf[self.row_capacity_offset..], size_of::<RowCapacity>())
    }

    pub fn get_schema(&self) -> Vec<ColSize> {
        let n_cols = self.get_n_cols() as usize;
        let mut schema = Vec::with_capacity(n_cols);
        let mut cursor = Cursor::new(&self.buf[self.schema_offset..]);
        for _ in 0..n_cols {
            schema.push(cursor.read_uint::<LittleEndian>(size_of::<ColSize>()).unwrap());
        }
        schema
    }

    pub fn set_n_rows(&mut self, n_rows: NRows) {
        Self::write::<NRows>(&mut self.buf[self.n_rows_offset..], n_rows as u64);
    }

    fn set_size(&mut self, size: HeaderSize) {
        Self::write::<HeaderSize>(&mut self.buf[self.header_size_offset..], size as u64);
    }

    fn set_n_cols(&mut self, n_cols: NCols) {
        Self::write::<NCols>(&mut self.buf[self.n_cols_offset..], n_cols as u64);
    }

    fn set_row_size(&mut self, row_size: RowSize) {
        Self::write::<RowSize>(&mut self.buf[self.row_size_offset..], row_size as u64);
    }

    fn set_row_capacity(&mut self, row_capacity: RowCapacity) {
        Self::write::<RowCapacity>(&mut self.buf[self.row_capacity_offset..], row_capacity as u64);
    }

    fn set_schema(&self, schema: &[usize]) {
        let mut offset = self.schema_offset;

    }

    fn write<T>(buf: &mut [u8], val: u64) {
        LittleEndian::write_uint(buf, val, size_of::<T>());
    }

}

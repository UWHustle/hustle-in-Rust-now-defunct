use block::{BLOCK_SIZE};
use std::ops::DerefMut;
use byteorder::{LittleEndian, ByteOrder};
use std::mem::size_of;

// These types must be safely casted to usize.
type NRows = u32;
type NCols = u32;
type RowSize = u32;
type ColSize = u32;
type RowCapacity = u32;

const N_ROWS_OFFSET: usize = 0;
const N_COLS_OFFSET: usize = N_ROWS_OFFSET + size_of::<NRows>();
const ROW_SIZE_OFFSET: usize = N_COLS_OFFSET + size_of::<NCols>();
const ROW_CAPACITY_OFFSET: usize = ROW_SIZE_OFFSET + size_of::<RowSize>();
const SCHEMA_OFFSET: usize = ROW_CAPACITY_OFFSET + size_of::<RowCapacity>();

#[derive(Clone)]
pub struct Header<D> {
    buf: D,
}

impl<D> Header<D> where D: DerefMut<Target = [u8]> {
    pub fn new(schema: &[usize], buf: D) -> Self {
        // Blocks must have valid schema.
        assert!(!schema.is_empty(), "Cannot create a block with empty schema");
        assert!(
            schema.iter().all(|col_size| *col_size > 0),
            "Column size must be positive integer",
        );

        let mut header = Self::with_buf(buf);
        let n_cols = schema.len();
        let header_size = SCHEMA_OFFSET + n_cols * size_of::<ColSize>();
        let row_size: usize = schema.iter().sum();

        // Calculate row capacity, accounting for two bits per row in the bitmap.
        let remaining_block_size = BLOCK_SIZE - header_size;
        let block_bits = remaining_block_size * 8;
        let row_bits = row_size * 8 + 2;
        let row_capacity = block_bits / row_bits - (block_bits % row_bits == 0) as usize;

        assert!(row_capacity > 0, "Row is too large to fit in a single block");

        header.set_n_rows(0);
        header.set_n_cols(n_cols);
        header.set_row_size(row_size);
        header.set_row_capacity(row_capacity);
        header.set_schema(schema);

        header
    }

    pub fn with_buf(buf: D) -> Self {
        Header {
            buf,
        }
    }

    pub fn size(&self) -> usize {
        SCHEMA_OFFSET + self.get_n_cols() * size_of::<ColSize>()
    }

    pub fn get_n_rows(&self) -> usize {
        Self::read::<NRows>(&self.buf[N_ROWS_OFFSET..]) as usize
    }

    pub fn get_n_cols(&self) -> usize {
        Self::read::<NCols>(&self.buf[N_COLS_OFFSET..]) as usize
    }

    pub fn get_row_size(&self) -> usize {
        Self::read::<RowSize>(&self.buf[ROW_SIZE_OFFSET..]) as usize
    }

    pub fn get_row_capacity(&self) -> usize {
        Self::read::<RowCapacity>(&self.buf[ROW_CAPACITY_OFFSET..]) as usize
    }

    pub fn get_schema(&self) -> Vec<usize> {
        let mut offset = SCHEMA_OFFSET;
        let n_cols = self.get_n_cols() as usize;
        let mut schema = Vec::with_capacity(n_cols);
        for _ in 0..n_cols {
            schema.push(Self::read::<ColSize>(&self.buf[offset..]) as usize);
            offset += size_of::<ColSize>()
        }
        schema
    }

    pub fn get_bitmap_size(&self) -> usize {
        self.get_row_capacity() / 8 + (self.get_row_capacity() % 8 != 0) as usize
    }

    pub fn set_n_rows(&mut self, n_rows: usize) {
        Self::write::<NRows>(&mut self.buf[N_ROWS_OFFSET..], n_rows as u64);
    }

    fn set_n_cols(&mut self, n_cols: usize) {
        Self::write::<NCols>(&mut self.buf[N_COLS_OFFSET..], n_cols as u64);
    }

    fn set_row_size(&mut self, row_size: usize) {
        Self::write::<RowSize>(&mut self.buf[ROW_SIZE_OFFSET..], row_size as u64);
    }

    fn set_row_capacity(&mut self, row_capacity: usize) {
        Self::write::<RowCapacity>(&mut self.buf[ROW_CAPACITY_OFFSET..], row_capacity as u64);
    }

    fn set_schema(&mut self, schema: &[usize]) {
        let mut offset = SCHEMA_OFFSET;
        for col in schema {
            Self::write::<ColSize>(&mut self.buf[offset..], *col as u64);
            offset += size_of::<ColSize>()
        }
    }

    fn read<T>(buf: &[u8]) -> u64 {
        LittleEndian::read_uint(buf, size_of::<T>())
    }

    fn write<T>(buf: &mut [u8], val: u64) {
        LittleEndian::write_uint(buf, val, size_of::<T>());
    }
}

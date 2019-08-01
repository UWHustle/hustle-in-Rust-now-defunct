use block::{BLOCK_SIZE, RawSlice};
use byteorder::{LittleEndian, ByteOrder};
use std::mem::size_of;
use std::sync::{MutexGuard, Mutex};

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

pub struct Header {
    n_rows: Mutex<RawSlice>,
    n_cols: RawSlice,
    row_size: RawSlice,
    row_capacity: RawSlice,
    schema: RawSlice,
}

impl Header {
    pub fn new(schema: &[usize], buf: &mut [u8]) -> Self {
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

        header.get_n_rows_guard().set(0);
        header.set_n_cols(n_cols);
        header.set_row_size(row_size);
        header.set_row_capacity(row_capacity);
        header.set_schema(schema);

        header
    }

    pub fn with_buf(buf: &mut [u8]) -> Self {
        let n_rows = Mutex::new(RawSlice::new(&mut buf[N_ROWS_OFFSET..N_COLS_OFFSET]));
        let n_cols = RawSlice::new(&mut buf[N_COLS_OFFSET..ROW_SIZE_OFFSET]);
        let row_size = RawSlice::new(&mut buf[ROW_SIZE_OFFSET..ROW_CAPACITY_OFFSET]);
        let row_capacity = RawSlice::new(&mut buf[ROW_CAPACITY_OFFSET..SCHEMA_OFFSET]);
        let schema = RawSlice::new(&mut buf[SCHEMA_OFFSET..]);

        Header {
            n_rows,
            n_cols,
            row_size,
            row_capacity,
            schema,
        }
    }

    pub fn size(&self) -> usize {
        SCHEMA_OFFSET + self.get_n_cols() * size_of::<ColSize>()
    }

    pub fn get_n_rows_guard(&self) -> NRowsGuard {
        NRowsGuard { inner: self.n_rows.lock().unwrap() }
    }

    pub fn get_n_cols(&self) -> usize {
        Self::read::<NCols>(&self.n_cols) as usize
    }

    pub fn get_row_size(&self) -> usize {
        Self::read::<RowSize>(&self.row_size) as usize
    }

    pub fn get_row_capacity(&self) -> usize {
        Self::read::<RowCapacity>(&self.row_capacity) as usize
    }

    pub fn get_schema(&self) -> Vec<usize> {
        let n_cols = self.get_n_cols() as usize;
        let mut schema = Vec::with_capacity(n_cols);
        for offset in (0..n_cols * size_of::<ColSize>()).step_by(size_of::<ColSize>()) {
            schema.push(Self::read::<ColSize>(&self.schema[offset..]) as usize);
        }
        schema
    }

    pub fn get_bitmap_size(&self) -> usize {
        self.get_row_capacity() / 8 + (self.get_row_capacity() % 8 != 0) as usize
    }

    fn set_n_cols(&mut self, n_cols: usize) {
        Self::write::<NCols>(&mut self.n_cols, n_cols as u64);
    }

    fn set_row_size(&mut self, row_size: usize) {
        Self::write::<RowSize>(&mut self.row_size, row_size as u64);
    }

    fn set_row_capacity(&mut self, row_capacity: usize) {
        Self::write::<RowCapacity>(&mut self.row_capacity, row_capacity as u64);
    }

    fn set_schema(&mut self, schema: &[usize]) {
        for (col, offset) in schema.iter()
            .zip((0..schema.len() * size_of::<ColSize>()).step_by(size_of::<ColSize>()))
        {
            Self::write::<ColSize>(&mut self.schema[offset..], *col as u64);
        }
    }

    fn read<T>(buf: &[u8]) -> u64 {
        LittleEndian::read_uint(buf, size_of::<T>())
    }

    fn write<T>(buf: &mut [u8], val: u64) {
        LittleEndian::write_uint(buf, val, size_of::<T>());
    }
}

pub struct NRowsGuard<'a> {
    inner: MutexGuard<'a, RawSlice>,
}

impl<'a> NRowsGuard<'a> {
    pub fn get(&self) -> usize {
        Header::read::<NRows>(&self.inner) as usize
    }

    pub fn set(&mut self, n_rows: usize) {
        Header::write::<NRows>(&mut self.inner, n_rows as u64);
    }
}

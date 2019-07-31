use std::{mem, slice, ptr};
use block::{BLOCK_SIZE};

#[derive(Clone)]
pub struct Header {
    n_rows: *mut usize,
    n_cols: *mut usize,
    row_size: *mut usize,
    row_capacity: *mut usize,
    schema: *mut usize,
}

impl Header {
    pub fn new(schema: &[usize], buf: &mut [u8]) -> Self {
        // Blocks must have valid schema.
        assert!(!schema.is_empty(), "Cannot create a block with empty schema");
        assert!(*schema.first().unwrap() > 0, "Column size must be positive integer");

        let n_rows = 0;
        let n_cols = schema.len();
        let row_size: usize = schema.iter().sum();
        let mut row_capacity = 0;

        let remaining_block_size = BLOCK_SIZE
            - mem::size_of_val(&n_rows)
            - mem::size_of_val(&n_cols)
            - mem::size_of_val(&row_size)
            - mem::size_of_val(&row_capacity)
            - n_cols * mem::size_of_val(schema.first().unwrap())
            - 2 * mem::size_of::<u64>(); // Padding to account for the bitmap blocks.

        // Calculate row capacity, accounting for two bits per row in the bitmap.
        row_capacity = remaining_block_size * 8 / (row_size * 8 + 2);

        assert!(row_capacity > 0, "Row is too large to fit in a single block");

        let header = Self::try_from_slice(buf).unwrap();

        unsafe {
            *header.n_rows = n_rows;
            *header.n_cols = n_cols;
            *header.row_size = row_size;
            *header.row_capacity = row_capacity;
            let schema_slice = slice::from_raw_parts_mut(header.schema, n_cols);
            schema_slice.copy_from_slice(schema);
        }

        header
    }

    pub fn try_from_slice(buf: &mut [u8]) -> Option<Self> {
        let mut ptrs = [ptr::null_mut(); 5];
        let mut offset = 0;
        for i in 0..ptrs.len() {
            ptrs[i] = buf.get_mut(offset..)?.as_mut_ptr() as *mut usize;
            offset += mem::size_of::<usize>();
        }

        let [n_rows, n_cols, row_size, row_capacity, schema] = ptrs;

        Some(Header {
            n_rows,
            n_cols,
            row_size,
            row_capacity,
            schema,
        })
    }

    pub fn size(&self) -> usize {
        unsafe {
            mem::size_of_val(&*self.n_rows) + mem::size_of_val(&*self.n_cols)
                + mem::size_of_val(&*self.row_size) + mem::size_of_val(&*self.row_capacity)
                + self.get_n_cols() * mem::size_of_val(&*self.schema)
        }
    }

    pub fn get_n_rows(&self) -> usize {
        unsafe { *self.n_rows }
    }

    pub fn get_n_cols(&self) -> usize {
        unsafe { *self.n_cols }
    }

    pub fn get_row_size(&self) -> usize {
        unsafe { *self.row_size }
    }

    pub fn get_row_capacity(&self) -> usize {
        unsafe { *self.row_capacity }
    }

    pub fn get_schema(&self) -> &[usize] {
        unsafe { slice::from_raw_parts(self.schema, *self.n_cols) }
    }

    pub fn set_n_rows(&self, n_rows: usize) {
        unsafe { *self.n_rows = n_rows };
    }
}

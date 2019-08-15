use std::mem::size_of;
use std::slice;

type NRows = u32;
type NCols = u32;
type ColSize = usize;

const N_ROWS_OFFSET: usize = 0;
const N_COLS_OFFSET: usize = N_ROWS_OFFSET + size_of::<NRows>();
const SCHEMA_OFFSET: usize = N_COLS_OFFSET + size_of::<NCols>();

pub struct Header {
    n_rows: *const NRows,
    n_cols: *const NCols,
    schema: *const ColSize,
    len: usize,
}

impl Header {
    pub fn new(schema: &[ColSize], buf: &mut [u8]) -> Self {
        let n_cols = schema.len();

        assert!(n_cols > 0, "Cannot create a block with empty schema");

        let row_size = schema.iter()
            .map(|&col_size| {
                assert!(col_size > 0, "Column size must be a positive integer");
                col_size as usize
            })
            .sum::<usize>();

        let len = SCHEMA_OFFSET + n_cols * size_of::<ColSize>();
        let n_rows = (buf.len() - len) / row_size;

        let (n_rows_raw, n_cols_raw, schema_raw) = Self::bind(buf);

        unsafe {
            *n_rows_raw = n_rows as NRows;
            *n_cols_raw = n_cols as NCols;
            slice::from_raw_parts_mut(schema_raw, n_cols).copy_from_slice(schema);
        }

        Header {
            n_rows: n_rows_raw,
            n_cols: n_cols_raw,
            schema: schema_raw,
            len,
        }
    }

    pub fn from_buf(buf: &mut [u8]) -> Self {
        let (n_rows, n_cols, schema) = Self::bind(buf);
        let size = SCHEMA_OFFSET + unsafe { *n_cols } as usize * size_of::<ColSize>();

        Header {
            n_rows,
            n_cols,
            schema,
            len: size,
        }
    }

    fn bind(buf: &mut [u8]) -> (*mut NRows, *mut NCols, *mut ColSize) {
        let n_rows = buf[N_ROWS_OFFSET..].as_ptr() as *mut NRows;
        let n_cols = buf[N_COLS_OFFSET..].as_ptr() as *mut NCols;
        let schema = buf[SCHEMA_OFFSET..].as_ptr() as *mut ColSize;
        (n_rows, n_cols, schema)
    }

    pub unsafe fn get_n_rows(&self) -> NRows {
        *self.n_rows
    }

    pub unsafe fn get_n_cols(&self) -> NCols {
        *self.n_cols
    }

    pub unsafe fn get_schema(&self) -> &[ColSize] {
        slice::from_raw_parts(self.schema, self.get_n_cols() as usize)
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

use types::data_type::DataType;
use std::{slice, fmt};
use types::borrowed_buffer::BorrowedBuffer;
use types::Buffer;
use types::integer::Int8;

pub struct HustleResult {
    data_types: Vec<DataType>,
    rows: Vec<Vec<Vec<u8>>>,
}

impl HustleResult {
    pub fn new(data_types: Vec<DataType>, rows: Vec<Vec<Vec<u8>>>) -> Self {
        HustleResult {
            data_types,
            rows,
        }
    }

    pub fn rows(&self) -> Iter {
        Iter::new(&self.data_types, self.rows.iter())
    }
}

impl fmt::Display for HustleResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        for row in self.rows() {
            let mut col_i = 0;
            while let Some(value) = row.get_col(col_i) {
                write!(
                    f,
                    "|{value:>width$}",
                    value = value.to_string(),
                    width = 5
                )?;
                col_i += 1;
            }
            writeln!(f, "|")?;
        }
        Ok(())
    }
}

pub struct Iter<'a> {
    data_types: &'a Vec<DataType>,
    row_iter: slice::Iter<'a, Vec<Vec<u8>>>
}

impl<'a> Iter<'a> {
    fn new(data_types: &'a Vec<DataType>, row_iter: slice::Iter<'a, Vec<Vec<u8>>>) -> Self {
        Iter {
            data_types,
            row_iter
        }
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = HustleRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.row_iter.next()
            .map(|row| HustleRow::new(self.data_types, row))
    }
}

pub struct HustleRow<'a> {
    data_types: &'a Vec<DataType>,
    row: &'a Vec<Vec<u8>>
}

impl<'a> HustleRow<'a> {
    fn new(data_types: &'a Vec<DataType>, row: &'a Vec<Vec<u8>>) -> Self {
        HustleRow {
            data_types,
            row
        }
    }

    fn get_i64(&self, col: usize) -> Option<i64> {
        self.get_col(col).map(|c| types::cast_value::<Int8>(c.as_ref()).value())
    }

    fn get_col(&self, col: usize) -> Option<Box<types::Value>> {
        self.row.get(col).and_then(|data| {
            self.data_types.get(col).map(|data_type| {
                let buff = BorrowedBuffer::new(data, data_type.clone(), false);
                buff.marshall()
            })
        })
    }
}

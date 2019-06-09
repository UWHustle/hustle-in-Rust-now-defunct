use types::data_type::DataType;
use std::{slice, fmt};
use types::borrowed_buffer::BorrowedBuffer;
use types::Buffer;
use types::integer::Int8;

pub struct HustleResult {
    schema: Vec<(String, DataType)>,
    rows: Vec<Vec<Vec<u8>>>,
}

impl HustleResult {
    pub fn new(schema: Vec<(String, DataType)>, rows: Vec<Vec<Vec<u8>>>) -> Self {
        HustleResult {
            schema,
            rows,
        }
    }

    pub fn rows(&self) -> Iter {
        Iter::new(&self.schema, self.rows.iter())
    }
}

impl fmt::Display for HustleResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        for (column_name, _) in &self.schema {
            write!(f, "|{value:>width$}", value = column_name, width = 5)?;
        }
        writeln!(f, "|")?;

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
    schema: &'a Vec<(String, DataType)>,
    row_iter: slice::Iter<'a, Vec<Vec<u8>>>
}

impl<'a> Iter<'a> {
    fn new(schema: &'a Vec<(String, DataType)>, row_iter: slice::Iter<'a, Vec<Vec<u8>>>) -> Self {
        Iter {
            schema,
            row_iter
        }
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = HustleRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.row_iter.next()
            .map(|row| HustleRow::new(self.schema, row))
    }
}

pub struct HustleRow<'a> {
    schema: &'a Vec<(String, DataType)>,
    row: &'a Vec<Vec<u8>>
}

impl<'a> HustleRow<'a> {
    fn new(schema: &'a Vec<(String, DataType)>, row: &'a Vec<Vec<u8>>) -> Self {
        HustleRow {
            schema,
            row
        }
    }

    fn get_i64(&self, col: usize) -> Option<i64> {
        self.get_col(col).map(|c| types::cast_value::<Int8>(c.as_ref()).value())
    }

    fn get_col(&self, col: usize) -> Option<Box<types::Value>> {
        self.row.get(col).and_then(|data| {
            self.schema.get(col).map(|(_, data_type)| {
                let buff = BorrowedBuffer::new(data, data_type.clone(), false);
                buff.marshall()
            })
        })
    }
}

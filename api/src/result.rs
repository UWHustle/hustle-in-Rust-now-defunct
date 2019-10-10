use std::fmt;
use hustle_types::HustleType;
use hustle_types::Int64;

pub struct HustleRow<'a> {
    types: &'a [Box<dyn HustleType>],
    row: &'a [Vec<u8>],
}

impl<'a> HustleRow<'a> {
    fn new(types: &'a [Box<dyn HustleType>], row: &'a [Vec<u8>]) -> Self {
        HustleRow {
            types,
            row,
        }
    }

    pub fn get_i64(&self, col: usize) -> Option<i64> {
        self.types.get(col)
            .and_then(|t|
                t.downcast_ref::<Int64>()
                    .and_then(|v|
                        self.row.get(col)
                            .map(|buf| v.get(buf))
                    )
            )
    }
}

pub struct HustleResult {
    names: Vec<String>,
    types: Vec<Box<dyn HustleType>>,
    rows: Vec<Vec<Vec<u8>>>,
}

impl HustleResult {
    pub fn new(
        names: Vec<String>,
        types: Vec<Box<dyn HustleType>>,
        rows: Vec<Vec<Vec<u8>>>,
    ) -> Self {
        HustleResult {
            names,
            types,
            rows,
        }
    }

    pub fn get_row(&self, row: usize) -> Option<HustleRow> {
        self.rows.get(row)
            .map(|row| HustleRow::new(&self.types, row))
    }
}

impl fmt::Display for HustleResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        for name in &self.names {
            write!(f, "|{value:>width$}", value = name, width = 5)?;
        }
        writeln!(f, "|")?;

        for row in &self.rows {
            for (buf, hustle_type) in row.iter().zip(&self.types) {
                write!(
                    f,
                    "|{value:>width$}",
                    value = hustle_type.to_string(buf),
                    width = 5
                )?;
            }
            writeln!(f, "|")?;
        }
        Ok(())
    }
}

use std::fmt;
use hustle_types::HustleType;

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
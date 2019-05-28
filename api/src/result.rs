use execution::{ExecutionEngine, type_system};
use execution::logical_entities::relation::Relation;
use execution::type_system::borrowed_buffer::BorrowedBuffer;
use execution::type_system::Buffer;
use std::fmt;

pub struct HustleResult<'a> {
    relation: Relation,
    execution_engine: &'a ExecutionEngine,
    data: Vec<u8>,
    row_index: usize,
    initialized: bool
}

impl<'a> HustleResult<'a> {
    pub fn new(relation: Relation, execution_engine: &'a ExecutionEngine) -> Self {
        // TODO: Read in rows with a generator instead of in bulk.
        // Due to some borrowing issues, it's easier to just read in all the rows of the relation
        // at once. For large tables, this could cause problems. This should be changed when
        // Rust generators become stable.
        let data = execution_engine
            .get_storage_manager()
            .relational_engine()
            .get(&relation.get_name())
            .expect("Error: Result relation does not exist")
            .bulk_read();

        HustleResult {
            relation,
            execution_engine,
            data,
            row_index: 0,
            initialized: false
        }
    }

    /// After the first time this function is called, the cursor will be at the first row in the
    /// relation. Each subsequent call advances the cursor forward one row. Returns true if the
    /// cursor is in bounds.
    pub fn step(&mut self) -> bool {
        if self.initialized {
            self.row_index += 1;
        } else {
            self.row_index = 0;
            self.initialized = true;
        }

        return self.row_index * self.relation.get_row_size() < self.data.len()
    }

    pub fn get_col(&self, col: usize) -> Option<Box<type_system::Value>> {
        let column = self.relation.get_columns().get(col)?;
        let offset = self.row_index * self.relation.get_row_size();
        let data = self.data.get(offset..offset + column.get_size())?;
        let buff = BorrowedBuffer::new(data, column.data_type(), false);
        Some(buff.marshall())
    }
}

impl<'a> fmt::Display for HustleResult<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let schema = self.relation.get_schema();
        let width = 5;

        for column in schema.get_columns() {
            write!(f, "|{value:>width$}", value = column.get_name(), width = width)?;
        }
        writeln!(f, "|")?;

        let physical_relation = self.execution_engine
            .get_storage_manager()
            .relational_engine()
            .get(self.relation.get_name())
            .unwrap();

        for block in physical_relation.blocks() {
            for row_i in 0..block.get_n_rows() {
                for col_i in 0..schema.get_columns().len() {
                    let data = block.get_row_col(row_i, col_i).unwrap();
                    let data_type = schema.get_columns()[col_i].data_type();
                    let buff = BorrowedBuffer::new(&data, data_type, false);
                    write!(
                        f,
                        "|{value:>width$}",
                        value = buff.marshall().to_string(),
                        width = width
                    )?;
                }
                writeln!(f, "|")?;
            }
        }

        Ok(())
    }
}

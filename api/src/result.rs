use execution::{ExecutionEngine, type_system};
use execution::logical_entities::relation::Relation;
use execution::type_system::borrowed_buffer::BorrowedBuffer;
use execution::type_system::Buffer;

pub struct HustleResult {
    relation: Relation,
    data: Vec<u8>,
    row_index: usize,
    initialized: bool
}

impl HustleResult {
    pub fn new(relation: Relation, execution_engine: &ExecutionEngine) -> Self {
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

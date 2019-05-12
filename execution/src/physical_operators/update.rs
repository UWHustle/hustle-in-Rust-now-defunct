use logical_entities::relation::Relation;
use logical_entities::predicates::Predicate;
use physical_operators::Operator;

use super::storage::StorageManager;
use type_system::borrowed_buffer::BorrowedBuffer;
use type_system::{Buffer, Value};
use logical_entities::row::Row;
use logical_entities::column::Column;

pub struct Update {
    relation: Relation,
    predicate: Option<Box<Predicate>>,
    cols: Vec<Column>,
    assignments: Vec<Box<Value>>
}

impl Update {
    pub fn new(
        relation: Relation,
        predicate: Option<Box<Predicate>>,
        cols: Vec<Column>,
        assignments: Vec<Box<Value>>
    ) -> Self {
        Update {
            relation,
            predicate,
            cols,
            assignments
        }
    }
}

impl Operator for Update {
    fn get_target_relation(&self) -> Relation {
        self.relation.clone()
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Relation, String> {
        let schema = self.relation.get_schema();
        let schema_sizes = schema.to_size_vec();
        let physical_relation = storage_manager
            .get_with_schema(self.relation.get_name(), &schema_sizes)
            .unwrap();

        // Indices of the columns in the relation.
        let mut cols_i = vec![];
        for col in &self.cols {
            let i = schema
                .get_columns()
                .iter()
                .position(|x| x == col)
                .unwrap();
            cols_i.push(i);
        }

        for block in physical_relation.blocks() {
            for row_i in 0..block.len() {
                // Assemble values in the current row (this is very inefficient!).
                let mut values = vec![];
                for col_i in 0..schema.get_columns().len() {
                    let data = block.get_row_col(row_i, col_i).unwrap();
                    let data_type = schema.get_columns()[col_i].data_type();
                    let buff = BorrowedBuffer::new(&data, data_type, false);
                    values.push(buff.marshall());
                }

                // Check whether the current row satisfies the predicate.
                let row = Row::new(schema.clone(), values);
                if let Some(ref predicate) = self.predicate {
                    if !predicate.evaluate(&row) {
                        continue;
                    }
                }

                // Update the columns with the new values.
                for (col_i, assignment) in cols_i.iter().zip(self.assignments.iter()) {
                    block.set_row_col(row_i, col_i.clone(), assignment.un_marshall().data())
                }
            }
        }

        Ok(self.get_target_relation())
    }
}
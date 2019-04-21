use logical_entities::column::Column;
use logical_entities::predicates::tautology::*;
use logical_entities::predicates::*;
use logical_entities::relation::Relation;
use logical_entities::row::Row;
use logical_entities::schema::Schema;
use physical_operators::Operator;
use type_system::borrowed_buffer::*;
use type_system::*;

use std::collections::HashMap;

use super::storage::StorageManager;

pub struct Project {
    relation: Relation,
    output_relation: Relation,
    predicate: Box<Predicate>,
}

impl Project {
    pub fn new(relation: Relation, output_cols: Vec<Column>, predicate: Box<Predicate>) -> Self {
        let schema = Schema::new(output_cols);
        let output_relation = Relation::new(&format!("{}_project", relation.get_name()), schema);

        Project {
            relation,
            output_relation,
            predicate,
        }
    }

    pub fn pure_project(relation: Relation, output_cols: Vec<Column>) -> Self {
        Self::new(relation, output_cols, Box::new(Tautology::new()))
    }
}

impl Operator for Project {
    fn get_target_relation(&self) -> Relation {
        self.output_relation.clone()
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Relation, String> {
        let input_data = storage_manager.get(self.relation.get_name()).unwrap();

        // Future optimization: create uninitialized Vec (this may require unsafe Rust)
        let mut output_data: Vec<u8> = vec![0; self.relation.get_total_size(storage_manager)];

        let mut i = 0; // Beginning of the current row in the input buffer
        let mut k = 0; // Current position in the input buffer
        let mut j = 0; // Current position in the output buffer

        // Loop over all the data
        let input_cols = self.relation.get_columns().to_vec();
        while i < input_data.len() {
            // Check whether the current row satisfies the predicate
            let mut values: Vec<Box<Value>> = vec![];
            for column in &input_cols {
                let value_len = column.get_datatype().next_size(&input_data[k..]);
                let value = BorrowedBuffer::new(
                    &input_data[k..k + value_len],
                    column.get_datatype(),
                    false,
                )
                .marshall();
                values.push(value);
                k += value_len;
            }
            let row = Row::new(self.relation.get_schema().clone(), values);
            let filter = self.predicate.evaluate(&row);

            // Filter columns if the predicate is true for this row
            if filter {
                // Place all values in the current row in a HashMap by column
                let mut col_map = HashMap::new();
                k = i;
                for column in &input_cols {
                    let value_len = column.get_datatype().next_size(&input_data[k..]);
                    col_map.insert(column, &input_data[k..k + value_len]);
                    k += value_len;
                }

                // Output values in the order of the output columns
                for column in self.output_relation.get_columns() {
                    let slice = col_map[column];
                    output_data[j..j + slice.len()].clone_from_slice(slice);
                    j += slice.len();
                }
            }
            i = k;
        }

        output_data.resize(j, 0);
        storage_manager.put(self.output_relation.get_name(), &output_data);

        Ok(self.get_target_relation())
    }
}

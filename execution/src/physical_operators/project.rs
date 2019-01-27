use logical_entities::column::Column;
use logical_entities::relation::Relation;
use logical_entities::schema::Schema;
use physical_operators::Operator;
use storage_manager::StorageManager;

use std::collections::HashMap;

#[derive(Debug)]
pub struct Project {
    relation: Relation,
    output_relation: Relation,
    predicate_name: String,
    comparator: i8,
    comp_value: Vec<u8>,
}

impl Project {
    pub fn new(relation: Relation, output_cols: Vec<Column>, predicate_name: String, comparator: i8, comp_value: Vec<u8>) -> Self {
        let schema = Schema::new(output_cols);
        let output_relation = Relation::new(format!("{}_project", relation.get_name()), schema);

        Project {
            relation,
            output_relation,
            predicate_name,
            comparator,
            comp_value,
        }
    }

    pub fn pure_project(relation: Relation, output_cols: Vec<Column>) -> Self {
        Self::new(relation, output_cols, String::new(), 2, vec!())
    }
}

impl Operator for Project {
    fn get_target_relation(&self) -> Relation {
        self.output_relation.clone()
    }

    fn execute(&self) -> Relation {
        let input_cols = self.relation.get_columns().to_vec();
        let input_data = StorageManager::get_full_data(&self.relation.clone());
        let output_cols = self.output_relation.get_columns().to_vec();
        let mut output_data = StorageManager::create_relation(&self.output_relation, self.relation.get_total_size());

        let mut i = 0; // Beginning of the current row in the input buffer
        let mut k = 0; // Current position in the input buffer
        let mut j = 0; // Current position in the output buffer

        // Loop over all the data
        while i < input_data.len() {

            // Check whether the current row satisfies the predicate
            let mut filter = true;
            if self.comparator >= -1 && self.comparator <= 1 {
                for column in &input_cols {
                    let value_len = column.get_datatype().get_next_length(&input_data[k..]);
                    if column.get_name().to_string() == self.predicate_name {
                        let value = &input_data[k..k + value_len].to_vec();
                        if !(column.get_datatype().compare(value, &self.comp_value) == self.comparator) {
                            filter = false;
                        }
                    }
                    k += value_len;
                }
            }

            // Filter columns if the predicate is true for this row
            if filter {

                // Place all values in the current row in a HashMap by column
                let mut col_map = HashMap::new();
                k = i;
                for column in &input_cols {
                    let value_len = column.get_datatype().get_next_length(&input_data[k..]);
                    col_map.insert(column, &input_data[k..k + value_len]);
                    k += value_len;
                }

                // Output values in the order of the output columns
                for column in &output_cols {
                    let slice = col_map[column];
                    let value_length = column.get_datatype().get_next_length(slice);
                    output_data[j..j + value_length].clone_from_slice(slice);
                    j += value_length;
                }
            }
            i = k;
        }

        StorageManager::trim_relation(&self.output_relation, j);
        self.get_target_relation()
    }
}
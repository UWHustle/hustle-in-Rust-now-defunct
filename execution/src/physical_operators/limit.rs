/*
Limit physical operator

The output is the first limit rows of the input relation.
*/
use logical_entities::relation::Relation;
use logical_entities::schema::Schema;
use physical_operators::Operator;

use super::storage::StorageManager;

#[derive(Debug)]
pub struct Limit {
    relation: Relation,
    limit: u32,
    output_relation: Relation,
}

impl Limit {
    pub fn new(relation: Relation, limit: u32) -> Limit {
        let schema = Schema::new(relation.get_columns().clone());
        let output_relation = Relation::new(
            &format!("{}{}", relation.get_name(), "_limit".to_string()),
            schema,
        );
        Limit {
            relation,
            limit,
            output_relation,
        }
    }
}

impl Operator for Limit {
    fn get_target_relation(&self) -> Relation {
        self.output_relation.clone()
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Relation, String> {
        let input_data = storage_manager.get(self.relation.get_name()).unwrap();

        // Future optimization: create uninitialized Vec (this may require unsafe Rust)
        let output_size = self.relation.get_row_size() * (self.limit as usize);
        let mut output_data: Vec<u8> = vec![0; output_size];

        let mut i = 0;
        let mut records = 0;
        while i < input_data.len() && records < self.limit {
            for column in self.relation.get_columns() {
                let value_length = column.data_type().next_size(&input_data[i..]);
                output_data[i..i + value_length].clone_from_slice(&input_data[i..i + value_length]);
                i += value_length;
            }
            records += 1;
        }

        output_data.resize(i, 0);
        storage_manager.put(self.output_relation.get_name(), &output_data);

        Ok(self.get_target_relation())
    }
}

use logical_entities::relation::Relation;
use logical_entities::schema::Schema;
use physical_operators::Operator;

use super::storage::StorageManager;

#[derive(Debug)]
pub struct Limit {
    input_relation: Relation,
    output_relation: Relation,
    limit: usize,
}

impl Limit {
    pub fn new(input_relation: Relation, limit: usize) -> Limit {
        let schema = Schema::new(input_relation.get_columns().clone());
        let output_relation =
            Relation::new(&format!("{}_limit", input_relation.get_name()), schema);
        Limit {
            input_relation,
            output_relation,
            limit,
        }
    }
}

impl Operator for Limit {
    fn get_target_relation(&self) -> Relation {
        self.output_relation.clone()
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Relation, String> {
        let in_schema = self.input_relation.get_schema();
        let in_record = storage_manager
            .get_with_schema(self.input_relation.get_name(), &in_schema.to_size_vec())
            .unwrap();

        let mut n_rows = 0;
        for in_block in in_record.blocks() {
            for row_i in 0..in_block.len() {
                if n_rows >= self.limit {
                    return Ok(self.get_target_relation());
                }
                for col_i in 0..in_schema.get_columns().len() {
                    let data = in_block.get_row_col(row_i, col_i).unwrap();
                    storage_manager.append(self.output_relation.get_name(), data);
                }
                n_rows += 1;
            }
        }

        Ok(self.get_target_relation())
    }
}

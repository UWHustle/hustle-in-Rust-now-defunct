use logical_entities::relation::Relation;
use logical_entities::schema::Schema;
use physical_operators::Operator;

use super::hustle_storage::StorageManager;

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
    fn get_target_relation(&self) -> Option<Relation> {
        Some(self.output_relation.clone())
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Option<Relation>, String> {
        let in_physical_relation = storage_manager
            .relational_engine()
            .get(self.input_relation.get_name())
            .unwrap();

        storage_manager.relational_engine().drop(self.output_relation.get_name());
        let out_physical_relation = storage_manager.relational_engine().create(
            self.output_relation.get_name(),
            self.output_relation.get_schema().to_size_vec()
        );

        let mut n_rows = 0;
        for in_block in in_physical_relation.blocks() {
            for row_i in 0..in_block.get_n_rows() {
                if n_rows >= self.limit {
                    return Ok(self.get_target_relation());
                }
                let mut row_builder = out_physical_relation.insert_row();
                for col_i in 0..in_block.get_n_cols() {
                    let data = in_block.get_row_col(row_i, col_i).unwrap();
                    row_builder.push(data);
                }
                n_rows += 1;
            }
        }

        Ok(self.get_target_relation())
    }
}

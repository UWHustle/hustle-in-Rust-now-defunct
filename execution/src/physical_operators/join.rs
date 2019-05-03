use logical_entities::relation::Relation;
use logical_entities::schema::Schema;
use physical_operators::Operator;

use super::storage::StorageManager;

pub struct Join {
    relation_l: Relation,
    relation_r: Relation,
    output_relation: Relation,
}

impl Join {
    pub fn new(relation_l: Relation, relation_r: Relation) -> Self {
        let mut joined_cols = vec![];
        joined_cols.extend(relation_l.get_columns().clone());
        joined_cols.extend(relation_r.get_columns().clone());
        let output_relation = Relation::new(
            &format!("{}_j_{}", relation_l.get_name(), relation_r.get_name()),
            Schema::new(joined_cols),
        );
        Join {
            relation_l,
            relation_r,
            output_relation,
        }
    }
}

impl Operator for Join {
    fn get_target_relation(&self) -> Relation {
        self.output_relation.clone()
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Relation, String> {
        let l_schema = self.relation_l.get_schema();
        let l_schema_sizes = l_schema.to_size_vec();
        let l_record = storage_manager
            .get_with_schema(self.relation_l.get_name(), &l_schema_sizes)
            .unwrap();
        let r_schema = self.relation_l.get_schema();
        let r_schema_sizes = r_schema.to_size_vec();
        let r_record = storage_manager
            .get_with_schema(self.relation_r.get_name(), &r_schema_sizes)
            .unwrap();
        storage_manager.delete(self.output_relation.get_name());
        storage_manager.put(self.output_relation.get_name(), &[]);

        // Simple Cartesian product
        for l_block in l_record.blocks() {
            for l_row_i in 0..l_block.len() {
                for r_block in r_record.blocks() {
                    for r_row_i in 0..r_block.len() {
                        let mut joined_row = vec![];
                        for col_i in 0..l_schema.get_columns().len() {
                            let data = l_block.get_row_col(l_row_i, col_i).unwrap();
                            joined_row.extend_from_slice(data);
                        }
                        for col_i in 0..r_schema.get_columns().len() {
                            let data = r_block.get_row_col(r_row_i, col_i).unwrap();
                            joined_row.extend_from_slice(data);
                        }
                        storage_manager.append(self.output_relation.get_name(), &joined_row);
                    }
                }
            }
        }

        Ok(self.get_target_relation())
    }
}

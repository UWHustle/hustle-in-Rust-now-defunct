use logical_entities::relation::Relation;
use physical_operators::Operator;

use super::storage::StorageManager;

pub struct CreateTable {
    relation: Relation,
}

impl CreateTable {
    pub fn new(relation: Relation) -> Self {
        CreateTable { relation }
    }
}

impl Operator for CreateTable {
    fn get_target_relation(&self) -> Relation {
        self.relation.clone()
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Relation, String> {
        storage_manager
            .relational_engine()
            .create(self.relation.get_name(), self.relation.get_schema().to_size_vec());

        Ok(self.get_target_relation())
    }
}

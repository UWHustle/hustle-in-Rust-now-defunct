use logical_entities::relation::Relation;
use physical_operators::Operator;

use super::storage::StorageManager;

pub struct TableReference {
    relation: Relation,
}

impl TableReference {
    pub fn new(relation: Relation) -> Self {
        TableReference { relation }
    }
}

impl Operator for TableReference {
    fn get_target_relation(&self) -> Relation {
        self.relation.clone()
    }

    fn execute(&self, _storage_manager: &StorageManager) -> Relation {
        self.get_target_relation()
    }
}

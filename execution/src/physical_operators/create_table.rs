use logical_entities::relation::Relation;
use storage_manager::StorageManager;
use physical_operators::Operator;

pub struct CreateTable {
    relation: Relation
}

impl CreateTable {
    pub fn new(relation: Relation) -> Self {
        CreateTable {
            relation,
        }
    }
}

impl Operator for CreateTable {
    fn get_target_relation(&self) -> Relation {
        self.relation.clone()
    }

    fn execute(&self) -> Relation {
        let data = StorageManager::create_relation(&self.relation, 0);
        StorageManager::flush(&data);
        self.get_target_relation()
    }
}
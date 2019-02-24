use logical_entities::relation::Relation;
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

    /// This currently is just a pass-through because memmap doesn't like zero-length maps
    fn execute(&self) -> Relation {
//        let data = StorageManager::create_relation(&self.relation, 0);
//        StorageManager::flush(&data);
        self.get_target_relation()
    }
}
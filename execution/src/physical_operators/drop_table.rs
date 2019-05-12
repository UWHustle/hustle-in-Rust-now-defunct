use logical_entities::relation::Relation;
use logical_entities::schema::Schema;
use physical_operators::Operator;

use super::storage::StorageManager;

pub struct DropTable {
    name: String,
}

impl DropTable {
    pub fn new(name: &str) -> Self {
        DropTable {
            name: String::from(name),
        }
    }
}

impl Operator for DropTable {
    fn get_target_relation(&self) -> Relation {
        Relation::new(&self.name, Schema::new(vec![]))
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Relation, String> {
        storage_manager
            .relational_engine()
            .drop(&self.name);

        Ok(self.get_target_relation())
    }
}

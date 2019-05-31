use logical_entities::relation::Relation;
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
    fn get_target_relation(&self) -> Option<Relation> {
        None
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Option<Relation>, String> {
        storage_manager
            .relational_engine()
            .drop(&self.name);

        Ok(self.get_target_relation())
    }
}

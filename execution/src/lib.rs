pub mod logical_entities;
pub mod physical_operators;
pub mod physical_plan;
pub mod relational_api;
pub mod test_helpers;
pub mod type_system;

use physical_plan::parser::parse;
use storage::StorageManager;
use logical_entities::relation::Relation;

extern crate storage;

pub struct ExecutionEngine {
    storage_manager: StorageManager
}

impl ExecutionEngine {
    pub fn new() -> Self {
        ExecutionEngine {
            storage_manager: StorageManager::new()
        }
    }

    pub fn execute_plan(&self, plan_string: &str) -> Option<Relation> {
        let node = parse(plan_string);
        node.execute(&self.storage_manager);
        node.get_output_relation()
    }

    pub fn get_storage_manager(&self) -> &StorageManager {
        &self.storage_manager
    }
}

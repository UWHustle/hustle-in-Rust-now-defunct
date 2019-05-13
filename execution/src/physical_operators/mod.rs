pub mod aggregate;
pub mod create_table;
pub mod drop_table;
pub mod export_csv;
pub mod import_csv;
pub mod insert;
pub mod delete;
pub mod update;
pub mod join;
pub mod limit;
pub mod print;
pub mod project;
pub mod table_reference;

use logical_entities::relation::Relation;

extern crate storage;
use self::storage::StorageManager;

pub trait Operator {
    // Returns the information for what relation will be returned when execute is called.
    fn get_target_relation(&self) -> Relation;

    // Executes the operator and returns the relation containing the results.
    fn execute(&self, storage_manager: &StorageManager) -> Result<Relation, String>;
}

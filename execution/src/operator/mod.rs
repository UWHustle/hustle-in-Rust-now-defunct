use hustle_catalog::Table;
use hustle_storage::StorageManager;

pub trait Operator {
    fn execute(&self, storage_manager: &StorageManager) -> Result<Option<Table>, String>;
}

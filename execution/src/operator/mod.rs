use hustle_storage::StorageManager;

pub use create_table::CreateTable;
pub use drop_table::DropTable;
pub use project::Project;
pub use select::Select;

pub mod create_table;
pub mod drop_table;
pub mod project;
pub mod select;

pub trait Operator {
    fn execute(&self, storage_manager: &StorageManager);
}

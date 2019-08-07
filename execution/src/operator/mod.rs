use hustle_storage::StorageManager;
use hustle_catalog::Catalog;

pub use create_table::CreateTable;
pub use drop_table::DropTable;
pub use project::Project;
pub use select::Select;
pub use table_reference::TableReference;
pub use collect::Collect;

pub mod create_table;
pub mod drop_table;
pub mod project;
pub mod select;
pub mod table_reference;
pub mod collect;

pub trait Operator {
    fn execute(&self, storage_manager: &StorageManager, catalog: &Catalog);
}

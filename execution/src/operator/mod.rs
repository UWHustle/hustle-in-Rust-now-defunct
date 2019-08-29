use hustle_storage::StorageManager;
use hustle_catalog::Catalog;

pub use create_table::CreateTable;
pub use drop_table::DropTable;
pub use insert::Insert;
pub use update::Update;
pub use delete::Delete;
pub use select::Select;
pub use table_reference::TableReference;
pub use collect::Collect;
use downcast_rs::Downcast;

pub mod create_table;
pub mod drop_table;
pub mod insert;
pub mod update;
pub mod delete;
pub mod select;
pub mod table_reference;
pub mod collect;

pub trait Operator: Downcast {
    fn execute(&self, storage_manager: &StorageManager, catalog: &Catalog);
}

impl_downcast!(Operator);

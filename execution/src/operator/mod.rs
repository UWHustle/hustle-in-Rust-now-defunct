use downcast_rs::Downcast;

pub use cartesian::Cartesian;
pub use collect::Collect;
pub use create_table::CreateTable;
pub use delete::Delete;
pub use drop_table::DropTable;
use hustle_catalog::Catalog;
use hustle_storage::StorageManager;
pub use insert::Insert;
pub use project::Project;
pub use select::Select;
pub use table_reference::TableReference;
pub use update::Update;

pub mod create_table;
pub mod drop_table;
pub mod insert;
pub mod update;
pub mod delete;
pub mod select;
pub mod project;
pub mod cartesian;
pub mod hash_join;
pub mod table_reference;
pub mod collect;
mod util;

pub trait Operator: Downcast {
    fn execute(self: Box<Self>, storage_manager: &StorageManager, catalog: &Catalog);
}

impl_downcast!(Operator);

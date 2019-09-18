use downcast_rs::Downcast;

pub use begin_transaction::BeginTransaction;
pub use cartesian::Cartesian;
pub use hash_join::HashJoin;
pub use collect::Collect;
pub use commit_transaction::CommitTransaction;
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

pub mod begin_transaction;
pub mod cartesian;
pub mod collect;
pub mod commit_transaction;
pub mod create_table;
pub mod delete;
pub mod drop_table;
pub mod insert;
pub mod project;
pub mod select;
pub mod hash_join;
pub mod table_reference;
pub mod update;

mod util;

pub trait Operator: Downcast {
    fn execute(self: Box<Self>, storage_manager: &StorageManager, catalog: &Catalog);
}

impl_downcast!(Operator);

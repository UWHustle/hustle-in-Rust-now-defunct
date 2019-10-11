


pub use create_table::CreateTable;
pub use delete::Delete;
pub use drop_table::DropTable;
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
//pub mod cartesian;
pub mod table_reference;
mod util;

pub trait Operator : Send {
    fn push_work_orders(&self);
}

pub trait WorkOrder : Send {
    fn execute(&self);
}


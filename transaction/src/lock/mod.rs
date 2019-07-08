use value::ValueLock;
pub use predicate::PredicateLock;

pub mod value;
pub mod predicate;

#[derive(PartialEq)]
pub enum AccessMode { Read, Write }

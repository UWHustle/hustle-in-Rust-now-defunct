pub use predicate::PredicateLock;
use value::ValueLock;

pub mod value;
pub mod predicate;

#[derive(Clone, Debug, PartialEq)]
pub enum AccessMode { Read, Write }

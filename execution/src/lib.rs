#[macro_use]
extern crate downcast_rs;

pub use engine::ExecutionEngine;

mod engine;
pub mod operator;
pub mod router;

#[macro_use]
extern crate downcast_rs;

pub use engine::ExecutionEngine;

mod engine;
mod operator;
mod router;

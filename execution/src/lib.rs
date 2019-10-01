#[macro_use]
extern crate downcast_rs;

pub use engine::ExecutionEngine;

mod engine;
mod state;
mod operator;
mod router;

#[macro_use]
extern crate serde;

pub mod resolver;
pub mod catalog;

pub use crate::resolver::Resolver;
pub mod logical_entities;
pub mod physical_operators;
pub mod physical_plan;
pub mod relational_api;
pub mod test_helpers;
pub mod type_system;

use physical_plan::global_sm;
use physical_plan::parser::parse;

extern crate storage;

pub fn execute_plan(plan_string: &str) {
    let node = parse(plan_string);
    node.execute(global_sm::get());
}

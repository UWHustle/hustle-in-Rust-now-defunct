
pub mod logical_entities;
pub mod physical_operators;
pub mod storage_manager;
pub mod physical_plan;
pub mod test_helpers;
/*
extern crate serde;
extern crate serde_json;

use serde_json::Error;

use physical_plan::node::Node;

#[no_mangle]
pub extern fn execute_plan(json_dag: String) -> String{
    let root_dag_node: Node = serde_json::from_str(json_dag.as_str()).unwrap();
    root_dag_node.execute();
}*/
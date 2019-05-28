extern crate execution;
extern crate serde_json;
extern crate storage;

pub mod logical_entities;
pub mod physical_operators;
pub mod physical_plan;
pub mod relational_api;
pub mod test_helpers;
pub mod type_system;

use execution::test_helpers::generate_data::generate_t_hustle_and_sqlite;
use storage::StorageManager;

const RECORD_COUNT: usize = 50;

fn main() {
    generate_t_hustle_and_sqlite(&StorageManager::new(), RECORD_COUNT, true);
}

extern crate hustle_execution;
extern crate serde_json;
extern crate hustle_storage;
extern crate hustle_types;

pub mod logical_entities;
pub mod physical_operators;
pub mod physical_plan;
pub mod relational_api;
pub mod test_helpers;

use hustle_execution::test_helpers::generate_data::generate_t_hustle_and_sqlite;
use hustle_storage::StorageManager;

const RECORD_COUNT: usize = 50;

fn main() {
    generate_t_hustle_and_sqlite(&StorageManager::new(), RECORD_COUNT, true);
}

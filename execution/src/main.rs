pub mod logical_entities;
pub mod physical_operators;
pub mod storage_manager;
pub mod physical_plan;
pub mod type_system;
pub mod test_helpers;

use test_helpers::data_gen::*;

const RECORD_COUNT: usize = 30;

fn main() {
    generate_relation_t_into_hustle_and_sqlite3(RECORD_COUNT, true);
    generate_relation_a_into_hustle_and_sqlite3(RECORD_COUNT);
    generate_relation_b_into_hustle_and_sqlite3(RECORD_COUNT);
}
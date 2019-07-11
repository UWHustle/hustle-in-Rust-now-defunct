extern crate hustle_execution;
extern crate hustle_storage;
extern crate hustle_types;

use hustle_execution::test_helpers::generate_data::generate_t_hustle_and_sqlite;
use hustle_execution::test_helpers::hustle_queries::hustle_agg;
use hustle_execution::test_helpers::sqlite3::run_query_sqlite;
use hustle_types::data_type::{DataType, Variant};
use hustle_storage::StorageManager;

const RECORD_COUNT: usize = 10;

#[test]
fn test_sum_aggregate() {
    let storage_manager = StorageManager::new();
    let relation = generate_t_hustle_and_sqlite(&storage_manager, RECORD_COUNT, true);
    let hustle_value = hustle_agg(
        &storage_manager,
        relation,
        "a",
        DataType::new(Variant::Int8, true),
        "sum",
    );
    let sqlite_value = run_query_sqlite("SELECT SUM(t.a) FROM t;", "SUM(t.a)");
    assert_eq!(hustle_value, sqlite_value);
}

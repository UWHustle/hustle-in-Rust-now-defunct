extern crate execution;
extern crate storage;
extern crate types;

use execution::test_helpers::generate_data::generate_t_hustle_and_sqlite;
use execution::test_helpers::hustle_queries::hustle_agg;
use execution::test_helpers::sqlite3::run_query_sqlite;
use hustle_types::data_type::{DataType, Variant};
use storage::StorageManager;

const RECORD_COUNT: usize = 10;

#[test]
fn test_min_aggregate() {
    let storage_manager = StorageManager::new();
    let relation = generate_t_hustle_and_sqlite(&storage_manager, RECORD_COUNT, true);
    let hustle_value = hustle_agg(
        &storage_manager,
        relation,
        "a",
        DataType::new(Variant::Int4, true),
        "min",
    );
    let sqlite_value = run_query_sqlite("SELECT MIN(t.a) FROM t;", "MIN(t.a)");
    assert_eq!(hustle_value, sqlite_value);
}

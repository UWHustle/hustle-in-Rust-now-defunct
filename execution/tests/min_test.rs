extern crate execution;

use execution::physical_plan::global_sm;
use execution::test_helpers::generate_data::generate_t_hustle_and_sqlite;
use execution::test_helpers::hustle_queries::hustle_agg;
use execution::test_helpers::sqlite3::run_query_sqlite;
use execution::type_system::data_type::{DataType, Variant};

const RECORD_COUNT: usize = 10;

#[test]
fn test_min_aggregate() {
    let relation = generate_t_hustle_and_sqlite(global_sm::get(), RECORD_COUNT, true);
    let hustle_value = hustle_agg(
        global_sm::get(),
        relation,
        "a",
        DataType::new(Variant::Int4, true),
        "min",
    );
    let sqlite_value = run_query_sqlite("SELECT MIN(t.a) FROM t;", "MIN(t.a)");
    assert_eq!(hustle_value, sqlite_value);
}

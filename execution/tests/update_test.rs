extern crate execution;

use execution::test_helpers::generate_data::generate_t_hustle_and_sqlite;
use execution::physical_plan::global_sm;
use execution::logical_entities::predicates::comparison::Comparison;
use execution::type_system::operators::Comparator;
use execution::type_system::integer::Int4;
use execution::test_helpers::hustle_queries::hustle_update;
use execution::test_helpers::sqlite3::run_query_sqlite;

const RECORD_COUNT: usize = 10;

#[test]
fn test_update() {
    let relation = generate_t_hustle_and_sqlite(global_sm::get(), RECORD_COUNT, true);
    let column = relation.column_from_name("a").unwrap();
    let predicate = Box::new(Comparison::new(
        column.clone(),
        Comparator::Less,
        Box::new(Int4::from(50))
    ));
    let assignment = Box::new(Int4::from(-100));
    let hustle_value = hustle_update(global_sm::get(), relation, predicate, "a", assignment);
    let sqlite_value = run_query_sqlite(
        "UPDATE t SET a = -100 WHERE a < 50; SELECT SUM(t.a) FROM t;",
        "SUM(t.a)");
    assert_eq!(hustle_value, sqlite_value);
}
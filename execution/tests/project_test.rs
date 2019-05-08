extern crate execution;

use execution::logical_entities::predicates::comparison::Comparison;
use execution::physical_plan::global_sm;
use execution::test_helpers::generate_data::generate_t_hustle_and_sqlite;
use execution::test_helpers::hustle_queries::hustle_predicate;
use execution::test_helpers::sqlite3::run_query_sqlite;
use execution::type_system::integer::Int4;
use execution::type_system::operators::Comparator;

const RECORD_COUNT: usize = 10;

#[test]
fn test_project_predicate() {
    let relation = generate_t_hustle_and_sqlite(global_sm::get(), RECORD_COUNT, true);
    let column = relation.column_from_name("a").unwrap();
    let predicate = Box::new(Comparison::new(
        column.clone(),
        Comparator::Less,
        Box::new(Int4::from(50)),
    ));
    let hustle_value = hustle_predicate(global_sm::get(), relation, "a", predicate);
    let sqlite_value = run_query_sqlite("SELECT SUM(t.a) FROM t WHERE t.a < 50;", "SUM(t.a)");
    assert_eq!(hustle_value, sqlite_value);
}

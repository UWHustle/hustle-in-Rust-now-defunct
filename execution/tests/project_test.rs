extern crate hustle_execution;
extern crate hustle_storage;
extern crate hustle_types;

use hustle_execution::logical_entities::predicates::comparison::{Comparison, ComparisonOperand};
use hustle_execution::test_helpers::generate_data::generate_t_hustle_and_sqlite;
use hustle_execution::test_helpers::hustle_queries::hustle_predicate;
use hustle_execution::test_helpers::sqlite3::run_query_sqlite;
use hustle_types::integer::Int4;
use hustle_types::operators::Comparator;
use hustle_storage::StorageManager;

const RECORD_COUNT: usize = 10;

#[test]
fn test_project_predicate() {
    let storage_manager = StorageManager::new();
    let relation = generate_t_hustle_and_sqlite(&storage_manager, RECORD_COUNT, true);
    let column = relation.column_from_name("a").unwrap();
    let predicate = Box::new(Comparison::new(
        Comparator::Lt,
        column.clone(),
        ComparisonOperand::Value(Box::new(Int4::from(50))),
    ));
    let hustle_value = hustle_predicate(&storage_manager, relation, "a", predicate);
    let sqlite_value = run_query_sqlite("SELECT SUM(t.a) FROM t WHERE t.a < 50;", "SUM(t.a)");
    assert_eq!(hustle_value, sqlite_value);
}

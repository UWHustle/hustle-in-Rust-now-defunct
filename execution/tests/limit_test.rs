extern crate execution;
extern crate storage;
extern crate types;

use execution::physical_operators::limit::Limit;
use execution::physical_operators::Operator;
use execution::test_helpers::generate_data::generate_t_hustle_and_sqlite;
use execution::test_helpers::hustle_queries::hustle_agg;
use hustle_types::data_type::*;
use storage::StorageManager;

const RECORD_COUNT: usize = 10;

#[test]
fn test_limit() {
    let storage_manager = StorageManager::new();
    let input_relation = generate_t_hustle_and_sqlite(&storage_manager, RECORD_COUNT, true);
    let output_relation = Limit::new(input_relation, 5)
        .execute(&storage_manager)
        .unwrap();
    let n_rows = hustle_agg(
        &storage_manager,
        output_relation.unwrap(),
        "a",
        DataType::new(Variant::Int8, true),
        "count",
    );
    assert_eq!(n_rows, 5);
}

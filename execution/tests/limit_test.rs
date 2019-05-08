extern crate execution;

use execution::physical_operators::limit::Limit;
use execution::physical_operators::Operator;
use execution::physical_plan::global_sm;
use execution::test_helpers::generate_data::generate_t_hustle_and_sqlite;
use execution::test_helpers::hustle_queries::hustle_agg;
use execution::type_system::data_type::*;

const RECORD_COUNT: usize = 10;

#[test]
fn test_limit() {
    let input_relation = generate_t_hustle_and_sqlite(global_sm::get(), RECORD_COUNT, true);
    let output_relation = Limit::new(input_relation, 5)
        .execute(global_sm::get())
        .unwrap();
    let n_rows = hustle_agg(
        global_sm::get(),
        output_relation,
        "a",
        DataType::new(Variant::Int8, true),
        "count",
    );
    assert_eq!(n_rows, 5);
}

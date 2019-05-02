extern crate execution;

use execution::logical_entities::column::Column;
use execution::logical_entities::relation::Relation;
use execution::physical_operators::limit::Limit;
use execution::physical_plan::node::Node;
use execution::test_helpers::data_gen::generate_relation_t_into_hustle_and_sqlite3;
use execution::test_helpers::select_sum::SelectSum;
use execution::test_helpers::sqlite3::run_query_sqlite3;
use execution::type_system::data_type::*;

use std::rc::Rc;

extern crate storage;
use self::storage::StorageManager;

const RECORD_COUNT: usize = 10;

#[test]
fn test_limit() {
    let relation = generate_relation_t_into_hustle_and_sqlite3(RECORD_COUNT, true);
    let limit = (RECORD_COUNT - 2) as usize;
    let limit_relation = hustle_limit(relation.clone(), limit);
    let hustle_calculation = sum_column_hustle(limit_relation.clone(), "a");
    let sqlite3_calculation =
        run_query_sqlite3(&format!("SELECT a AS out FROM t LIMIT {};", limit), "out");
    assert_eq!(hustle_calculation, sqlite3_calculation);
}

fn sum_column_hustle(relation: Relation, column_name: &str) -> u128 {
    let select_operator = SelectSum::new(
        relation.clone(),
        Column::new(column_name, DataType::new(Variant::Int4, true)),
    );
    select_operator
        .execute(&StorageManager::new())
        .parse::<u128>()
        .unwrap()
}

fn hustle_limit(relation: Relation, limit: usize) -> Relation {
    let limit_op = Limit::new(relation, limit);
    let node = Node::new(Rc::new(limit_op), vec![]);
    node.execute(&StorageManager::new())
}

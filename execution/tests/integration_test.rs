extern crate execution;

use execution::logical_entities::column::Column;
use execution::logical_entities::relation::Relation;
use execution::physical_operators::join::Join;
use execution::physical_operators::Operator;
use execution::test_helpers::data_gen::generate_relation_t_into_hustle_and_sqlite3;
use execution::test_helpers::data_gen::insert_into_hustle;
use execution::test_helpers::select_sum::SelectSum;
use execution::test_helpers::sqlite3::run_query_sqlite3;
use execution::type_system::data_type::*;
use execution::type_system::integer::*;

extern crate csv;

extern crate storage;
use self::storage::StorageManager;

const RECORD_COUNT: usize = 1024;

#[test]
fn test_flow() {
    let relation = generate_relation_t_into_hustle_and_sqlite3(RECORD_COUNT, true);

    println!("Checkpoint 0");
    let hustle_calculation = sum_column_hustle(relation.clone(), "b");
    let sqlite3_calculation = run_query_sqlite3("SELECT SUM(b) FROM T;", "SUM(b)");
    assert_eq!(hustle_calculation, sqlite3_calculation);

    println!("Checkpoint 1");
    let join_relation = hustle_join(relation.clone(), relation.clone());
    let hustle_calculation = sum_column_hustle(join_relation.clone(), "b");
    let sqlite3_calculation = run_query_sqlite3(
        "SELECT SUM(t1.b)+SUM(t2.b) as Out FROM t as t1 JOIN t as t2;",
        "Out",
    );
    assert_eq!(hustle_calculation, sqlite3_calculation);

    println!("Checkpoint 2");
    let insert_value = Int4::from(3);
    println!("Checkpoint 2.6");
    insert_into_hustle(10, &insert_value, relation.clone());
    println!("Checkpoint 2.7");
    let hustle_calculation = sum_column_hustle(relation.clone(), "b");
    println!("Checkpoint 2.8");
    let sqlite3_calculation = run_query_sqlite3("SELECT SUM(b) FROM T;", "SUM(b)");
    println!("Checkpoint 2.9");
    assert_eq!(hustle_calculation, sqlite3_calculation + 30);
    println!("Checkpoint 3");
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

fn hustle_join(relation1: Relation, relation2: Relation) -> Relation {
    let join_operator = Join::new(relation1.clone(), relation2.clone());
    join_operator.execute(&StorageManager::new()).unwrap()
}

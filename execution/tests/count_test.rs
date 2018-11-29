extern crate execution;

use execution::test_helpers::sqlite3::run_query_sqlite3;
use execution::test_helpers::data_gen::generate_relation_into_hustle_and_sqlite3;

use execution::logical_entities::relation::Relation;
use execution::logical_entities::column::Column;

use execution::physical_operators::aggregate::Aggregate;
use execution::physical_operators::select_sum::SelectSum;

use execution::logical_entities::aggregations::count::Count;

const RECORD_COUNT: usize = 10;

use execution::physical_plan::node::Node;
use std::rc::Rc;

#[test]
fn test_dag_count_aggregate() {
    let relation = generate_relation_into_hustle_and_sqlite3(RECORD_COUNT);

    let aggregated_relation = hustle_count(relation.clone());
    let hustle_calculation = sum_column_hustle(aggregated_relation.clone(), "count".to_string());
    let sqlite3_calculation = run_query_sqlite3("SELECT COUNT(t.a) FROM t;", "COUNT(t.a)");
    assert_eq!(hustle_calculation, sqlite3_calculation);
}



fn sum_column_hustle(relation: Relation, column_name: String) -> u128 {
    let select_operator = SelectSum::new(relation.clone(),Column::new(column_name, "Int".to_string()));
    select_operator.execute().parse::<u128>().unwrap()
}

fn hustle_count(relation1:Relation) -> Relation {
    let aggregate_operator = Rc::new(Aggregate::new(Count::new(relation1.clone())));

    let root_node = Node::new(aggregate_operator, vec!());
    root_node.execute()
}
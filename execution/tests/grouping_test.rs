extern crate execution;

use execution::test_helpers::sqlite3::run_query_sqlite3;
use execution::test_helpers::data_gen::generate_relation_into_hustle_and_sqlite3;

use execution::logical_entities::aggregations::sum::Sum;
use execution::logical_entities::column::Column;
use execution::logical_entities::relation::Relation;

use execution::physical_operators::Operator;
use execution::physical_operators::aggregate::Aggregate;
use execution::physical_operators::print::Print;
use execution::physical_operators::select_sum::SelectSum;

const RECORD_COUNT: usize = 10;

use execution::physical_plan::node::Node;
use std::rc::Rc;

fn sum_column_hustle(relation: Relation, column_name: String) -> u128 {
    let select_operator = SelectSum::new(relation.clone(),Column::new(column_name, "Int".to_string()));
    select_operator.execute().parse::<u128>().unwrap()
}

#[test]
fn test_dag_sum_group_by_aggregate() {
    let relation = generate_relation_into_hustle_and_sqlite3(RECORD_COUNT);

    let aggregated_relation = hustle_sum_group_by(relation.clone());
    let hustle_calculation = sum_column_hustle(aggregated_relation.clone(), "SUM(a)".to_string());
    let sqlite3_calculation = run_query_sqlite3("SELECT t.b,SUM(t.a) FROM t GROUP BY t.b;","SUM(t.a)");
    assert_eq!(hustle_calculation, sqlite3_calculation);
}

fn hustle_sum_group_by(relation1:Relation) -> Relation {
    let aggregate_operator = Rc::new(Aggregate::new(Sum::new(relation1.clone(), Column::new("a".to_string(), "Int".to_string()))));
    let print_operator = Rc::new(Print::new(relation1.clone()));
    let print_operator_after = Rc::new(Print::new(aggregate_operator.get_target_relation()));

    let print_node_before = Rc::new(Node::new(print_operator, vec!()));

    let sum_node = Rc::new(Node::new(aggregate_operator.clone(), vec!(print_node_before)));

    let print_node = Rc::new(Node::new(print_operator_after, vec!(sum_node)));
    print_node.execute();
    aggregate_operator.get_target_relation()
}
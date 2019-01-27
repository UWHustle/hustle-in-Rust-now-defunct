extern crate execution;

use execution::test_helpers::sqlite3::run_query_sqlite3;
use execution::test_helpers::data_gen::generate_relation_into_hustle_and_sqlite3;

use execution::logical_entities::aggregations::sum::Sum;
use execution::logical_entities::column::Column;
use execution::logical_entities::relation::Relation;
use execution::logical_entities::types::integer::IntegerType;

use execution::physical_operators::Operator;
use execution::physical_operators::aggregate::Aggregate;
use execution::physical_operators::project::Project;
use execution::physical_operators::select_sum::SelectSum;

const RECORD_COUNT: usize = 10;

use execution::physical_plan::node::Node;
use std::rc::Rc;


fn sum_column_hustle(relation: Relation, column_name: String) -> u128 {
    let select_operator = SelectSum::new(relation.clone(),Column::new(column_name, "Int".to_string()));
    select_operator.execute().parse::<u128>().unwrap()
}

#[test]
fn test_dag_sum_aggregate() {
    let relation = generate_relation_into_hustle_and_sqlite3(RECORD_COUNT);

    let aggregated_relation = hustle_sum(relation.clone());
    let hustle_calculation = sum_column_hustle(aggregated_relation.clone(), "SUM(a)".to_string());
    let sqlite3_calculation = run_query_sqlite3("SELECT SUM(t.a) FROM t;", "SUM(t.a)");
    assert_eq!(hustle_calculation, sqlite3_calculation);
}


fn hustle_sum(relation1:Relation) -> Relation {
    let project_operator = Project::pure_project(relation1.clone(), vec!(Column::new("a".to_string(), "Int".to_string())));

    let agg_column = Column::new("a".to_string(), "Int".to_string());
    let sum_aggregation = Sum::new(agg_column.get_datatype());
    let aggregate_operator = Rc::new(Aggregate::new(project_operator.get_target_relation(), agg_column, vec!(), sum_aggregation));


    let project_node = Rc::new(Node::new(Rc::new(project_operator), vec!()));
    let sum_node = Rc::new(Node::new(aggregate_operator.clone(), vec!(project_node.clone())));
    sum_node.execute();
    aggregate_operator.get_target_relation()
}
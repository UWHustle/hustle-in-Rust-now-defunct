extern crate execution;

use execution::test_helpers::sqlite3::run_query_sqlite3;
use execution::test_helpers::data_gen::generate_relation_into_hustle_and_sqlite3;

use execution::logical_entities::relation::Relation;
use execution::logical_entities::column::Column;

use execution::physical_operators::select_sum::SelectSum;

use execution::physical_operators::project::Project;

use execution::logical_entities::types::integer::IntegerType;

use execution::physical_operators::print::Print;

use execution::physical_operators::Operator;

const RECORD_COUNT: usize = 10;

use execution::physical_plan::node::Node;
use std::rc::Rc;


fn sum_column_hustle(relation: Relation, column_name: String) -> u128 {
    let select_operator = SelectSum::new(relation.clone(),Column::new(column_name, "Int".to_string()));
    select_operator.execute().parse::<u128>().unwrap()
}

#[test]
fn test_project_predicate() {
    let relation = generate_relation_into_hustle_and_sqlite3(RECORD_COUNT);

    let predicated_relation = hustle_where(relation.clone());
    let hustle_calculation = sum_column_hustle(predicated_relation.clone(), "a".to_string());
    let sqlite3_calculation = run_query_sqlite3("SELECT SUM(t.a) FROM t WHERE t.a<50;", "SUM(t.a)");
    assert_eq!(hustle_calculation, sqlite3_calculation); // Add one to fail and see
}


fn hustle_where(relation1:Relation) -> Relation {
    let project_operator = Project::pure_project(relation1.clone(), vec!(Column::new("a".to_string(), "Int".to_string())));
    let print_operator = Rc::new(Print::new(project_operator.get_target_relation().clone()));
    let project_operator_after = Rc::new(Project::new(relation1.clone(), vec!(Column::new("a".to_string(), "Int".to_string())), "a".to_string(), -1, IntegerType::marshall(&50).0));
    let print_operator_after = Rc::new(Print::new(project_operator_after.get_target_relation().clone()));

   // let sum_aggregation = Sum::new(project_operator.get_target_relation(), Column::new("a".to_string(), "Int".to_string()));
    //let aggregate_operator = Rc::new(Aggregate::new( sum_aggregation));


    let project_node_before = Rc::new(Node::new(Rc::new(project_operator), vec!()));
    let print_node_before = Rc::new(Node::new(print_operator.clone(), vec!(project_node_before.clone())));
    let project_node_after = Rc::new(Node::new(project_operator_after.clone(), vec!(print_node_before.clone())));
    let print_node_after = Rc::new(Node::new(print_operator_after.clone(), vec!(project_node_after.clone())));
    print_node_after.execute();
    project_operator_after.get_target_relation()
}
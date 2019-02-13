extern crate execution;

use execution::logical_entities::aggregations::max::Max;
use execution::logical_entities::column::Column;
use execution::logical_entities::relation::Relation;
use execution::physical_operators::aggregate::Aggregate;
use execution::physical_operators::project::Project;
use execution::physical_operators::select_sum::SelectSum;
use execution::physical_plan::node::Node;
use execution::test_helpers::data_gen::generate_relation_into_hustle_and_sqlite3;
use execution::test_helpers::sqlite3::run_query_sqlite3;
use execution::type_system::type_id::*;

use std::rc::Rc;

const RECORD_COUNT: usize = 10;

#[test]
fn test_max_aggregate() {
    let relation = generate_relation_into_hustle_and_sqlite3(RECORD_COUNT);
    let agg_col = Column::new(String::from("a"), TypeID::new(Variant::Int4, true));
    let agg_relation = hustle_max(relation.clone(), agg_col);
    let hustle_calculation = sum_column_hustle(agg_relation.clone(), "MAX(a)".to_string());
    let sqlite3_calculation = run_query_sqlite3("SELECT MAX(t.a) FROM t;", "MAX(t.a)");
    assert_eq!(hustle_calculation, sqlite3_calculation);
}

fn sum_column_hustle(relation: Relation, column_name: String) -> u128 {
    let select_operator = SelectSum::new(relation.clone(), Column::new(column_name, TypeID::new(Variant::Int4, true)));
    select_operator.execute().parse::<u128>().unwrap()
}

fn hustle_max(relation: Relation, agg_col: Column) -> Relation {
    let project_op = Project::pure_project(relation, vec!(agg_col.clone()));
    let project_node = Node::new(Rc::new(project_op), vec!());
    let agg_op = Aggregate::new(project_node.get_output_relation(), agg_col.clone(), vec!(), Max::new(agg_col.get_datatype()));
    Node::new(Rc::new(agg_op), vec!(Rc::new(project_node))).execute()
}
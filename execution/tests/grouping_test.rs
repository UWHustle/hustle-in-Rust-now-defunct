extern crate execution;

use execution::logical_entities::aggregations::sum::Sum;
use execution::logical_entities::column::Column;
use execution::logical_entities::relation::Relation;
use execution::physical_operators::aggregate::Aggregate;
use execution::physical_operators::project::Project;
use execution::physical_operators::select_sum::SelectSum;
use execution::physical_plan::node::Node;
use execution::test_helpers::data_gen::generate_relation_t_into_hustle_and_sqlite3;
use execution::test_helpers::sqlite3::run_query_sqlite3;
use execution::type_system::data_type::*;

use std::rc::Rc;

extern crate storage;
use self::storage::StorageManager;

const RECORD_COUNT: usize = 10;

fn sum_column_hustle(relation: Relation, column_name: String) -> u128 {
    let select_operator = SelectSum::new(
        relation.clone(),
        Column::new(column_name, DataType::new(Variant::Int4, true)),
    );
    select_operator
        .execute(&StorageManager::new())
        .parse::<u128>()
        .unwrap()
}

#[test]
fn test_dag_sum_group_by_aggregate() {
    let relation = generate_relation_t_into_hustle_and_sqlite3(RECORD_COUNT, true);

    let aggregated_relation = hustle_sum_group_by(relation.clone());
    let hustle_calculation = sum_column_hustle(aggregated_relation.clone(), "SUM(a)".to_string());
    let sqlite3_calculation =
        run_query_sqlite3("SELECT t.b,SUM(t.a) FROM t GROUP BY t.b;", "SUM(t.a)");
    assert_eq!(hustle_calculation, sqlite3_calculation);
}

// TODO: This test as currently implemented has no group by columns
fn hustle_sum_group_by(relation: Relation) -> Relation {
    let col = relation.get_columns().get(0).unwrap().clone();
    let project_op = Project::pure_project(relation, vec![col.clone()]);
    let project_node = Node::new(Rc::new(project_op), vec![]);
    let aggregate_op = Aggregate::new(
        project_node.get_output_relation(),
        col.clone(),
        vec![col.clone()],
        Box::new(Sum::new(col.data_type())),
    );
    Node::new(Rc::new(aggregate_op), vec![Rc::new(project_node)]).execute(&StorageManager::new())
}

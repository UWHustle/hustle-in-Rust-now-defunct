extern crate execution;

use execution::logical_entities::column::Column;
use execution::logical_entities::predicates::comparison::*;
use execution::logical_entities::predicates::Predicate;
use execution::logical_entities::relation::Relation;
use execution::physical_operators::project::Project;
use execution::physical_operators::select_sum::SelectSum;
use execution::physical_plan::node::Node;
use execution::test_helpers::data_gen::generate_relation_t_into_hustle_and_sqlite3;
use execution::test_helpers::sqlite3::run_query_sqlite3;
use execution::type_system::integer::*;
use execution::type_system::operators::*;
use execution::type_system::type_id::*;

use std::rc::Rc;

const RECORD_COUNT: usize = 10;

fn sum_column_hustle(relation: Relation, column: Column) -> u128 {
    let select_operator = SelectSum::new(relation.clone(), column);
    select_operator.execute().parse::<u128>().unwrap()
}

#[test]
fn test_project_predicate() {
    let relation = generate_relation_t_into_hustle_and_sqlite3(RECORD_COUNT, false);
    let project_col = Column::new(String::from("a"), TypeID::new(Variant::Int4, true));
    let predicate = Comparison::new(
        project_col.clone(),
        Comparator::Less,
        Box::new(Int4::from(50)),
    );
    let predicated_relation = hustle_where(
        relation.clone(),
        vec![project_col.clone()],
        Box::new(predicate),
    );
    let hustle_calculation = sum_column_hustle(predicated_relation.clone(), project_col);
    let sqlite3_calculation =
        run_query_sqlite3("SELECT SUM(t.a) FROM t WHERE t.a < 50;", "SUM(t.a)");
    assert_eq!(hustle_calculation, sqlite3_calculation);
}

fn hustle_where(
    relation: Relation,
    projection: Vec<Column>,
    predicate: Box<Predicate>,
) -> Relation {
    let project_op = Project::new(relation.clone(), projection, predicate);
    Node::new(Rc::new(project_op), vec![]).execute()
}

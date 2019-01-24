extern crate execution;

use execution::test_helpers::sqlite3::run_query_sqlite3;
use execution::test_helpers::data_gen::generate_relation_into_hustle_and_sqlite3;

use execution::logical_entities::relation::Relation;
use execution::logical_entities::column::Column;

use execution::physical_operators::join::Join;
use execution::physical_operators::select_sum::SelectSum;

use execution::physical_operators::Operator;

const RECORD_COUNT: usize = 32;

use execution::physical_plan::node::Node;
use std::rc::Rc;


fn test_dag_double_join() {
 let relation = generate_relation_into_hustle_and_sqlite3(RECORD_COUNT);

 let join_relation = hustle_double_join(relation.clone(), relation.clone(), relation.clone());
 let hustle_calculation = sum_column_hustle(join_relation.clone(), "b".to_string());
 let sqlite3_calculation = run_query_sqlite3("SELECT SUM(t1.b)+SUM(t2.b)+SUM(t3.b) AS Out FROM t as t1 JOIN t as t2 JOIN t as t3;","Out");
 assert_eq!(hustle_calculation, sqlite3_calculation);

}

fn sum_column_hustle(relation: Relation, column_name: String) -> u128 {
    let select_operator = SelectSum::new(relation.clone(),Column::new(column_name, "Int".to_string()));
    select_operator.execute().parse::<u128>().unwrap()
}

fn hustle_double_join(relation1:Relation, relation2:Relation, relation3:Relation) -> Relation {
    let join_operator = Join::new(relation1.clone(), relation2.clone());
    let second_join_operator = Join::new(join_operator.get_target_relation().clone(), relation3.clone());
    let base_node = Node::new(Rc::new(join_operator), vec!());
    let root_node = Node::new(Rc::new(second_join_operator), vec!(Rc::new(base_node)));
    root_node.execute()
}

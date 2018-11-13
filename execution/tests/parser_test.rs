
/*

extern crate execution;

use execution::test_helpers::data_gen::generate_relation_into_hustle_and_sqlite3;

use execution::physical_plan::parser;

const RECORD_COUNT: usize = 32;


#[test]
fn test_dag_double_join() {

    generate_relation_into_hustle_and_sqlite3(RECORD_COUNT);

    let root_node = parser::parse(
"
Physical Plan
TopLevelPlan
+-plan=Selection[has_repartition=false]
| +-input=TableReference[relation=T,alias=test]
| | +-AttributeReference[id=0,name=a,relation=test,type=Int NULL]
| | +-AttributeReference[id=1,name=b,relation=test,type=Int NULL]
| +-project_expressions=
|   +-AttributeReference[id=0,name=a,relation=test,type=Int NULL]
|   +-AttributeReference[id=1,name=b,relation=test,type=Int NULL]
+-output_attributes=
  +-AttributeReference[id=0,name=a,relation=test,type=Int NULL]
  +-AttributeReference[id=1,name=b,relation=test,type=Int NULL]
");
    root_node.execute();

}
*/
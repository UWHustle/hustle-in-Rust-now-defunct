use physical_plan::node::Node;

use logical_entities::column::Column;
use logical_entities::relation::Relation;
use logical_entities::schema::Schema;

use logical_entities::types::integer::IntegerType;
use logical_entities::types::DataTypeTrait;

use physical_operators::print::Print;
use physical_operators::project::Project;
use physical_operators::Operator;

extern crate serde_json;

use self::serde_json::Value;

use std::rc::Rc;

pub fn parse(string_plan: &str) -> Node {
    let plan: Value = serde_json::from_str(string_plan).unwrap();

    let input_relation = &plan["plan"]["input"];

    let json_columns = input_relation[""].as_array().unwrap();
    let mut columns: Vec<Column> = vec![];
    for i in 0..json_columns.len() {
        let column = Column::new(
            json_columns[i]["name"].as_str().unwrap().to_string(),
            json_columns[i]["type"].as_str().unwrap().to_string(),
        );
        columns.push(column);
    }

    let name = input_relation["relation"].as_str().unwrap().to_string();
    let schema = Schema::new(columns);
    let input_relation = Relation::new(name.to_string(), schema);

    let output_relation = &plan["plan"]["project_expressions"];

    let json_columns = output_relation.as_array().unwrap();
    let mut columns: Vec<Column> = vec![];
    for i in 0..json_columns.len() {
        let column = Column::new(
            json_columns[i]["name"].as_str().unwrap().to_string(),
            json_columns[i]["type"].as_str().unwrap().to_string(),
        );
        columns.push(column);
    }

    let name = "output_relation";
    let schema = Schema::new(columns);
    let output_relation = Relation::new(name.to_string(), schema);

    let project_operator = Project::new(
        input_relation.clone(),
        output_relation.get_columns().clone(),
    );
    let print_operator = Print::new(project_operator.get_target_relation());

    let project_node = Node::new(Rc::new(project_operator), vec![]);
    return Node::new(Rc::new(print_operator), vec![Rc::new(project_node)]);
}

pub fn type_string_to_type(_type_string: &str) -> impl DataTypeTrait {
    return IntegerType;
}

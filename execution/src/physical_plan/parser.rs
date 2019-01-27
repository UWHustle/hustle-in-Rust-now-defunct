use logical_entities::column::Column;
use logical_entities::relation::Relation;
use logical_entities::schema::Schema;

use logical_entities::aggregations::sum::Sum;
use logical_entities::aggregations::count::Count;

use logical_entities::types::DataType;

use physical_plan::node::Node;

use physical_operators::aggregate::Aggregate;
use physical_operators::join::Join;
use physical_operators::print::Print;
use physical_operators::project::Project;
use physical_operators::table_reference::TableReference;

use std::rc::Rc;

extern crate serde_json;

use self::serde_json::Value;

pub fn parse(string_plan: &str) -> Node {
    let json: Value = serde_json::from_str(string_plan).unwrap();

    let root_node = parse_node(&json["plan"]);
    let print_operator = Print::new(root_node.get_output_relation());
    let print_node = Node::new(Rc::new(print_operator), vec!(Rc::new(root_node)));

    print_node
}

fn parse_node(json: &Value) -> Node {
    let str_name = json["json_name"].as_str().unwrap();
    match str_name {
        "TableReference" => parse_table_reference(json),
        "Selection" => parse_selection(json),
        "Aggregate" => parse_aggregate(json),
        "HashJoin" => parse_hash_join(json),
        _ => panic!("Optimizer tree node type {} not supported", str_name),
    }
}

/// Always computes the cross join; unfortunately parser/optimizer require an "ON" clause
fn parse_hash_join(json: &Value) -> Node {
    let left = parse_node(&json["left"]);
    let right = parse_node(&json["right"]);

    let join_operator = Join::new(left.get_output_relation(), right.get_output_relation());
    let join_node = Node::new(Rc::new(join_operator), vec!(Rc::new(left), Rc::new(right)));

    let project_attributes = parse_column_list(&json["project_expressions"]);
    let project_operator = Project::pure_project(join_node.get_output_relation(), project_attributes);
    Node::new(Rc::new(project_operator), vec!(Rc::new(join_node)))
}

fn parse_aggregate(json: &Value) -> Node {
    let input = parse_node(&json["input"]);
    let aggregate_function = &json["aggregate_expressions"].as_array().unwrap().get(0).unwrap()["aggregate_function"];
    let function_type = aggregate_function["function"].as_str().unwrap();

    let aggregate_attribute = parse_column(&aggregate_function["array"].as_array().unwrap().get(0).unwrap());
    let group_by_attributes = parse_column_list(&json["grouping_expressions"]);

    // Project onto the union of the aggregate column and the group by columns
    let mut project_attributes = group_by_attributes.clone();
    project_attributes.push(aggregate_attribute.clone());
    let project_operator = Project::pure_project(input.get_output_relation(), project_attributes);
    let project_node = Node::new(Rc::new(project_operator), vec!(Rc::new(input)));

    match function_type {
        "SUM" => {
            let sum_operator = Aggregate::new(project_node.get_output_relation(), aggregate_attribute.clone(), group_by_attributes, Sum::new(aggregate_attribute.get_datatype()));
            Node::new(Rc::new(sum_operator), vec!(Rc::new(project_node)))
        }
        "COUNT" => {
            let count_operator = Aggregate::new(project_node.get_output_relation(), aggregate_attribute.clone(), group_by_attributes, Count::new());
            Node::new(Rc::new(count_operator), vec!(Rc::new(project_node)))
        }
        _ => panic!("Aggregate function {} not supported", function_type),
    }
}

fn parse_selection(json: &Value) -> Node {
    let input = parse_node(&json["input"]);
    let project_attributes = parse_column_list(&json["project_expressions"]);

    let filter_predicate = &json["filter_predicate"];
    let project_operator: Project = match filter_predicate {
        Value::Null => Project::pure_project(input.get_output_relation(), project_attributes),
        _ => {
            let filter_attribute = parse_column(&filter_predicate["attribute_reference"]);
            let str_value = get_string(&filter_predicate["literal"]["value"]);

            let compare_value = DataType::Integer.parse_and_marshall(str_value);
            let comparison_str = filter_predicate["json_name"].as_str().unwrap();

            let comparator = match comparison_str {
                "Equal" => 0,
                "Greater" => 1,
                "Less" => -1,
                _ => panic!("Unknown comparison type {}", comparison_str)
            };
            Project::new(input.get_output_relation(), project_attributes,  filter_attribute.get_name().clone(), comparator, compare_value.0)
        }
    };

    Node::new(Rc::new(project_operator), vec!(Rc::new(input)))
}

fn parse_table_reference(json: &Value) -> Node {
    let columns = parse_column_list(&json["array"]);
    let name = get_string(&json["relation"]);
    let relation = Relation::new(name, Schema::new(columns));

    Node::new(Rc::new(TableReference::new(relation)), vec!())
}

fn parse_column_list(json: &Value) -> Vec<Column> {
    let json_columns = json.as_array().expect("Unable to extract columns");
    let mut columns: Vec<Column> = vec![];
    for i in 0..json_columns.len() {
        columns.push(parse_column(&json_columns[i]));
    }
    columns
}

fn parse_column(json: &Value) -> Column {
    let mut name = get_string(&json["name"]);
    if name == "" {
        name = get_string(&json["alias"]);
    }

    // Currently Long types are incorrectly interpreted as IP addresses so just use Int
    let mut typename = get_string(&json["type"]);
    if typename == "Long NULL" || typename == "Long" {
        typename = "Int".to_string();
    }

    Column::new(name, typename)
}

fn get_string(json: &Value) -> String {
    json.as_str().unwrap().to_string()
}
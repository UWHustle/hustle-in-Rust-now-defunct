use logical_entities::column::Column;
use logical_entities::predicates::comparison::*;
use logical_entities::predicates::connective::*;
use logical_entities::predicates::Predicate;
use logical_entities::relation::Relation;
use logical_entities::row::Row;
use logical_entities::schema::Schema;
use physical_operators::aggregate::Aggregate;
use physical_operators::create_table::CreateTable;
use physical_operators::drop_table::DropTable;
use physical_operators::insert::Insert;
use physical_operators::join::Join;
use physical_operators::limit::Limit;
use physical_operators::print::Print;
use physical_operators::project::Project;
use physical_operators::table_reference::TableReference;
use physical_plan::node::Node;
use type_system;
use type_system::data_type::*;
use type_system::operators::*;

extern crate serde_json;

use std::rc::Rc;

pub fn parse(string_plan: &str) -> Node {
    let json: serde_json::Value = serde_json::from_str(string_plan).unwrap();
    let root_node = parse_node(&json["plan"]);

    // We only want to print for a selection
    if &json["plan"]["json_name"] == "Selection" {
        let print_op = Print::new(root_node.get_output_relation());
        Node::new(Rc::new(print_op), vec![Rc::new(root_node)])
    } else {
        root_node
    }
}

fn parse_node(json: &serde_json::Value) -> Node {
    let json_name = json["json_name"].as_str().unwrap();
    match json_name {
        "TableReference" => parse_table_reference(json),
        "Selection" => parse_selection(json),
        "Aggregate" => parse_aggregate(json),
        "HashJoin" => parse_hash_join(json),
        "Limit" => parse_limit(json),
        "InsertTuple" => parse_insert_tuple(json),
        "CreateTable" => parse_create_table(json),
        "DropTable" => parse_drop_table(json),
        _ => panic!("Optimizer tree node type {} not supported", json_name),
    }
}

/// Always computes the cross join; unfortunately parser/optimizer require an "ON" clause
fn parse_hash_join(json: &serde_json::Value) -> Node {
    let left = parse_node(&json["left"]);
    let right = parse_node(&json["right"]);

    let join_op = Join::new(left.get_output_relation(), right.get_output_relation());
    let join_node = Node::new(Rc::new(join_op), vec![Rc::new(left), Rc::new(right)]);

    let project_cols = parse_columns(&json["project_expressions"]);
    let project_op = Project::pure_project(join_node.get_output_relation(), project_cols);
    Node::new(Rc::new(project_op), vec![Rc::new(join_node)])
}

fn parse_aggregate(json: &serde_json::Value) -> Node {
    let input = parse_node(&json["input"]);

    let agg_json = &json["aggregate_expressions"]
        .as_array()
        .unwrap()
        .get(0)
        .unwrap()["aggregate_function"];
    let agg_col_in = parse_column(&agg_json["array"].get(0).unwrap()["type"]);
    let agg_name = agg_json["function"].as_str().unwrap();
    let agg_out_name = format!("{}({})", agg_name, agg_col_in.get_name());

    let mut output_col_names = parse_column_names(&json["grouping_expressions"]);
    output_col_names.push(agg_out_name);

    let agg_op = Aggregate::from_str(
        input.get_output_relation(),
        agg_col_in,
        &agg_out_name,
        output_col_names,
        agg_name,
    )
    .unwrap();
    Node::new(Rc::new(agg_op), vec![Rc::new(input)])
}

fn parse_insert_tuple(json: &serde_json::Value) -> Node {
    let input = parse_node(&json["input"]);
    let relation = input.get_output_relation();
    let values = parse_value_list(&json["column_values"]);
    let row = Row::new(relation.get_schema().clone(), values);
    let insert_op = Insert::new(relation, row);
    Node::new(Rc::new(insert_op), vec![Rc::new(input)])
}

fn parse_connective_predicate(json: &serde_json::Value) -> Connective {
    let connective_type = ConnectiveType::from_str(json["json_name"].as_str().unwrap());
    let json_terms = json["array"]
        .as_array()
        .expect("Unable to extract predicate terms");
    let mut terms: Vec<Box<Predicate>> = vec![];
    for term in json_terms {
        terms.push(parse_predicate(term));
    }
    Connective::new(connective_type, terms)
}

fn parse_comparison_predicate(json: &serde_json::Value) -> Comparison {
    let comp_value_str = get_string(&json["literal"]["value"]);
    let comp_value_type = DataType::from_str(json["literal"]["type"].as_str().unwrap()).unwrap();
    let comp_value = comp_value_type.parse(&comp_value_str).unwrap();

    let comparator_str = json["json_name"].as_str().unwrap();
    let comparator = Comparator::from_str(comparator_str).unwrap();
    let filter_col = parse_column(&json["attribute_reference"]);
    let predicate = Comparison::new(filter_col, comparator, comp_value);
    predicate
}

fn parse_predicate(json: &serde_json::Value) -> Box<Predicate> {
    let json_name = json["json_name"].as_str().unwrap();
    match json_name {
        "Equal" | "Less" | "LessOrEqual" | "Greater" | "GreaterOrEqual" => {
            Box::new(parse_comparison_predicate(json))
        }
        "And" | "Or" => Box::new(parse_connective_predicate(json)),
        _ => panic!("Unknown predicate type {}", json_name),
    }
}

fn parse_selection(json: &serde_json::Value) -> Node {
    let input = parse_node(&json["input"]);
    let output_cols = parse_columns(&json["project_expressions"]);

    let filter_predicate = &json["filter_predicate"];
    let project_op = match filter_predicate {
        serde_json::Value::Null => Project::pure_project(input.get_output_relation(), output_cols),
        _ => Project::new(
            input.get_output_relation(),
            output_cols,
            parse_predicate(filter_predicate),
        ),
    };
    Node::new(Rc::new(project_op), vec![Rc::new(input)])
}

fn parse_create_table(json: &serde_json::Value) -> Node {
    let cols = parse_columns(&json["attributes"]);
    let relation = Relation::new(&json["relation"].as_str().unwrap(), Schema::new(cols));
    Node::new(Rc::new(CreateTable::new(relation)), vec![])
}

fn parse_drop_table(json: &serde_json::Value) -> Node {
    Node::new(
        Rc::new(DropTable::new(&json["relation"].as_str().unwrap())),
        vec![],
    )
}

fn parse_table_reference(json: &serde_json::Value) -> Node {
    let cols = parse_columns(&json["array"]);
    let relation = Relation::new(&json["relation"].as_str().unwrap(), Schema::new(cols));
    Node::new(Rc::new(TableReference::new(relation)), vec![])
}

fn parse_columns(json: &serde_json::Value) -> Vec<Column> {
    let json_cols = json.as_array().expect("Unable to extract columns");
    let mut columns: Vec<Column> = vec![];
    for column in json_cols {
        columns.push(parse_column(column));
    }
    columns
}

fn parse_column_names(json: &serde_json::Value) -> Vec<String> {
    let json_cols = json.as_array().expect("Unable to extract columns");
    let mut names: Vec<String> = vec![];
    for column in json_cols {
        names.push(parse_column_name(column).to_string());
    }
    names
}

fn parse_column(json: &serde_json::Value) -> Column {
    Column::new(
        parse_column_name(json),
        DataType::from_str(&json["type"].as_str().unwrap()).unwrap(),
    )
}

fn parse_column_name(json: &serde_json::Value) -> &str {
    let mut name = &json["name"].as_str().unwrap();
    if name.len() == 0 {
        name = &json["alias"].as_str().unwrap();
    }
    name
}

fn parse_value_list(json: &serde_json::Value) -> Vec<Box<type_system::Value>> {
    let json_values = json.as_array().expect("Unable to extract values");
    let mut values: Vec<Box<type_system::Value>> = vec![];
    for value in json_values {
        values.push(parse_value(value));
    }
    values
}

fn parse_value(json: &serde_json::Value) -> Box<type_system::Value> {
    let data_type = DataType::from_str(&json["type"].as_str().unwrap()).unwrap();
    data_type.parse(&json["value"].as_str().unwrap()).unwrap()
}

fn parse_limit(json: &serde_json::Value) -> Node {
    let input = parse_node(&json["input"]);
    let limit = json.as_str().unwrap().parse::<usize>().unwrap();
    let limit_operator = Limit::new(input.get_output_relation(), limit);
    Node::new(Rc::new(limit_operator), vec![Rc::new(input)])
}

fn get_string(json: &serde_json::Value) -> String {
    json.as_str().unwrap().to_string()
}

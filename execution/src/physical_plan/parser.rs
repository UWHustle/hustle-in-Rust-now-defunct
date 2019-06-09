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
use physical_operators::update::Update;
use physical_operators::join::Join;
use physical_operators::limit::Limit;
use physical_operators::project::Project;
use physical_operators::table_reference::TableReference;
use physical_plan::node::Node;
use types;
use types::data_type::*;
use types::operators::*;

extern crate serde_json;

use std::rc::Rc;
use physical_operators::delete::Delete;

pub fn parse(string_plan: &str) -> Node {
    let json: serde_json::Value = serde_json::from_str(string_plan).unwrap();
    parse_node(&json["plan"])
}

fn parse_node(json: &serde_json::Value) -> Node {
    let json_name = json["json_name"].as_str().unwrap();
    match json_name {
        "TableReference" => parse_table_reference(json),
        "Selection" => parse_selection(json),
        "Aggregate" => parse_aggregate(json),
        "HashJoin" | "NestedLoopsJoin" => parse_join(json),
        "Limit" => parse_limit(json),
        "InsertTuple" => parse_insert_tuple(json),
        "DeleteTuples" => parse_delete_tuples(json),
        "UpdateTable" => parse_update_table(json),
        "CreateTable" => parse_create_table(json),
        "DropTable" => parse_drop_table(json),
        _ => panic!("Optimizer tree node type {} not supported", json_name),
    }
}

fn parse_join(json: &serde_json::Value) -> Node {
    let left = parse_node(&json["left"]);
    let right = parse_node(&json["right"]);

    let join_op = match &json["left_join_attributes"] {
        serde_json::Value::Null => Join::new(
            left.get_output_relation().unwrap(),
            right.get_output_relation().unwrap(),
            vec![],
            vec![],
        ),
        _ => {
            let l_attributes = parse_columns(&json["left_join_attributes"]);
            let r_attributes = parse_columns(&json["right_join_attributes"]);
            Join::new(
                left.get_output_relation().unwrap(),
                right.get_output_relation().unwrap(),
                l_attributes,
                r_attributes,
            )
        }
    };
    let join_node = Node::new(Rc::new(join_op), vec![Rc::new(left), Rc::new(right)]);

    let project_cols = parse_columns(&json["project_expressions"]);
    let project_op = Project::pure_project(
        join_node.get_output_relation().unwrap(),
        project_cols);
    Node::new(Rc::new(project_op), vec![Rc::new(join_node)])
}

fn parse_aggregate(json: &serde_json::Value) -> Node {
    let input = parse_node(&json["input"]);

    let agg_json = &json["aggregate_expressions"]
        .as_array()
        .unwrap()
        .get(0)
        .unwrap();

    let agg_col_in = parse_column(
        &agg_json["aggregate_function"]["array"]
            .as_array()
            .unwrap()
            .get(0)
            .unwrap(),
    );
    let agg_name = agg_json["aggregate_function"]["function"].as_str().unwrap();
    let agg_out_name = format!("{}({})", agg_name, agg_col_in.get_name());
    let agg_out_type = DataType::from_str(&agg_json["type"].as_str().unwrap()).unwrap();
    let agg_col_out = Column::new(&agg_out_name, agg_out_type);
    let mut output_col_names = parse_column_names(&json["grouping_expressions"]);
    output_col_names.push(agg_out_name.clone());

    let agg_op = Aggregate::from_str(
        input.get_output_relation().unwrap(),
        agg_col_in,
        agg_col_out,
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
    let row = Row::new(relation.as_ref().unwrap().get_schema().clone(), values);
    let insert_op = Insert::new(relation.unwrap(), row);
    Node::new(Rc::new(insert_op), vec![Rc::new(input)])
}

fn parse_delete_tuples(json: &serde_json::Value) -> Node {
    let input = parse_node(&json["input"]);
    let predicate = json.get("predicate").map(|p| parse_predicate(p));
    let delete_op = Delete::new(input.get_output_relation().unwrap(), predicate);
    Node::new(Rc::new(delete_op), vec![Rc::new(input)])
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
        serde_json::Value::Null => Project::pure_project(input.get_output_relation().unwrap(),
                                                         output_cols),
        _ => Project::new(
            input.get_output_relation().unwrap(),
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

fn parse_update_table(json: &serde_json::Value) -> Node {
    let input = parse_node(&json["input"]);
    let predicate = json.get("predicate").map(|p| parse_predicate(p));
    let cols = parse_columns(&json["attributes"]);
    let assignments = parse_value_list(&json["assigned_values"]);
    let update_op = Update::new(input.get_output_relation().unwrap(), predicate,
                                cols, assignments);
    Node::new(Rc::new(update_op), vec![Rc::new(input)])
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
        &parse_column_name(json),
        DataType::from_str(&json["type"].as_str().unwrap()).unwrap(),
    )
}

fn parse_column_name(json: &serde_json::Value) -> String {
    let mut name = get_string(&json["name"]);
    if name.len() == 0 {
        name = get_string(&json["alias"]);
    }
    name
}

fn parse_value_list(json: &serde_json::Value) -> Vec<Box<types::Value>> {
    let json_values = json.as_array().expect("Unable to extract values");
    let mut values: Vec<Box<types::Value>> = vec![];
    for value in json_values {
        values.push(parse_value(value));
    }
    values
}

fn parse_value(json: &serde_json::Value) -> Box<types::Value> {
    let data_type = DataType::from_str(&json["type"].as_str().unwrap()).unwrap();
    data_type.parse(&json["value"].as_str().unwrap()).unwrap()
}

fn parse_limit(json: &serde_json::Value) -> Node {
    let input = parse_node(&json["input"]);
    let limit = json["limit"].as_str().unwrap().parse::<usize>().unwrap();
    let limit_operator = Limit::new(input.get_output_relation().unwrap(), limit);
    Node::new(Rc::new(limit_operator), vec![Rc::new(input)])
}

fn get_string(json: &serde_json::Value) -> String {
    json.as_str().unwrap().to_string()
}

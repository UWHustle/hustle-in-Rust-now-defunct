use logical_entities::aggregations::avg::Avg;
use logical_entities::aggregations::count::Count;
use logical_entities::aggregations::max::Max;
use logical_entities::aggregations::min::Min;
use logical_entities::aggregations::sum::Sum;
use logical_entities::column::Column;
use logical_entities::relation::Relation;
use logical_entities::schema::Schema;
use physical_operators::aggregate::Aggregate;
use physical_operators::join::Join;
use physical_operators::limit::Limit;
use physical_operators::print::Print;
use physical_operators::project::Project;
use physical_operators::table_reference::TableReference;
use physical_plan::node::Node;
use type_system::operators::*;
use type_system::type_id::*;
use physical_operators::insert::Insert;
use logical_entities::row::Row;
use type_system;

extern crate serde_json;
use std::rc::Rc;

pub fn parse(string_plan: &str) -> Node {
    let json: serde_json::Value = serde_json::from_str(string_plan).unwrap();

    let root_node = parse_node(&json["plan"]);
    let print_op = Print::new(root_node.get_output_relation());
    Node::new(Rc::new(print_op), vec![Rc::new(root_node)])
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
        _ => panic!("Optimizer tree node type {} not supported", json_name),
    }
}

/// Always computes the cross join; unfortunately parser/optimizer require an "ON" clause
fn parse_hash_join(json: &serde_json::Value) -> Node {
    let left = parse_node(&json["left"]);
    let right = parse_node(&json["right"]);

    let join_op = Join::new(left.get_output_relation(), right.get_output_relation());
    let join_node = Node::new(Rc::new(join_op), vec![Rc::new(left), Rc::new(right)]);

    let project_cols = parse_column_list(&json["project_expressions"]);
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
    let agg_col = parse_column(&agg_json["array"].as_array().unwrap().get(0).unwrap());

    // Project onto the union of the aggregate column and the group by columns
    let group_by_cols = parse_column_list(&json["grouping_expressions"]);
    let mut project_cols = group_by_cols.clone();
    project_cols.push(agg_col.clone());
    let project_op = Project::pure_project(input.get_output_relation(), project_cols);
    let project_node = Node::new(Rc::new(project_op), vec![Rc::new(input)]);

    let agg_type = TypeID::from_string(get_string(
        &json["aggregate_expressions"]
            .as_array()
            .unwrap()
            .get(0)
            .unwrap()["type"],
    ));
    let agg_name = agg_json["function"].as_str().unwrap();
    match agg_name {
        "AVG" => {
            let avg_op = Aggregate::new(
                project_node.get_output_relation(),
                agg_col.clone(),
                group_by_cols,
                Avg::new(agg_type),
            );
            Node::new(Rc::new(avg_op), vec![Rc::new(project_node)])
        }
        "COUNT" => {
            let count_op = Aggregate::new(
                project_node.get_output_relation(),
                agg_col.clone(),
                group_by_cols,
                Count::new(agg_type),
            );
            Node::new(Rc::new(count_op), vec![Rc::new(project_node)])
        }
        "MAX" => {
            let max_op = Aggregate::new(
                project_node.get_output_relation(),
                agg_col.clone(),
                group_by_cols,
                Max::new(agg_type),
            );
            Node::new(Rc::new(max_op), vec![Rc::new(project_node)])
        }
        "MIN" => {
            let min_op = Aggregate::new(
                project_node.get_output_relation(),
                agg_col.clone(),
                group_by_cols,
                Min::new(agg_type),
            );
            Node::new(Rc::new(min_op), vec![Rc::new(project_node)])
        }
        "SUM" => {
            let sum_op = Aggregate::new(
                project_node.get_output_relation(),
                agg_col.clone(),
                group_by_cols,
                Sum::new(agg_type),
            );
            Node::new(Rc::new(sum_op), vec![Rc::new(project_node)])
        }
        _ => {
            panic!("Aggregate function {} not supported", agg_name);
        }
    }
}

fn parse_insert_tuple(json: &serde_json::Value) -> Node {
    let input = parse_node(&json["input"]);
    let relation = input.get_output_relation();
    let values = parse_value_list(&json["column_values"]);
    let row = Row::new(relation.get_schema().clone(), values);
    let insert_op = Insert::new(relation, row);
    Node::new(Rc::new(insert_op), vec!(Rc::new(input)))
}

fn parse_selection(json: &serde_json::Value) -> Node {
    let input = parse_node(&json["input"]);
    let output_cols = parse_column_list(&json["project_expressions"]);

    let filter_predicate = &json["filter_predicate"];
    let project_op = match filter_predicate {
        serde_json::Value::Null => Project::pure_project(input.get_output_relation(), output_cols),
        _ => {
            let comp_value_str = get_string(&filter_predicate["literal"]["value"]);
            let comp_value_type =
                TypeID::from_string(get_string(&filter_predicate["literal"]["type"]));
            let comp_value = comp_value_type.parse(&comp_value_str);

            let comparator_str = filter_predicate["json_name"].as_str().unwrap();
            let comparator = match comparator_str {
                "Equal" => Comparator::Equal,
                "Less" => Comparator::Less,
                "LessOrEqual" => Comparator::LessEq,
                "Greater" => Comparator::Greater,
                "GreaterOrEqual" => Comparator::GreaterEq,
                _ => panic!("Unknown comparison type {}", comparator_str),
            };
            let filter_col = parse_column(&filter_predicate["attribute_reference"]);
            Project::new(
                input.get_output_relation(),
                output_cols,
                filter_col.get_name(),
                true,
                comparator,
                &*comp_value,
            )
        }
    };
    Node::new(Rc::new(project_op), vec![Rc::new(input)])
}

fn parse_table_reference(json: &serde_json::Value) -> Node {
    let cols = parse_column_list(&json["array"]);
    let relation = Relation::new(get_string(&json["relation"]), Schema::new(cols));
    Node::new(Rc::new(TableReference::new(relation)), vec![])
}

fn parse_column_list(json: &serde_json::Value) -> Vec<Column> {
    let json_cols = json.as_array().expect("Unable to extract columns");
    let mut columns: Vec<Column> = vec![];
    for column in json_cols {
        columns.push(parse_column(column));
    }
    columns
}

fn parse_column(json: &serde_json::Value) -> Column {
    let mut name = get_string(&json["name"]);
    if name == "" {
        name = get_string(&json["alias"]);
    }
    Column::new(name, TypeID::from_string(get_string(&json["type"])))
}

fn parse_value_list(json: &serde_json::Value) -> Vec<Box<type_system::Value>> {
    let json_values = json.as_array().expect("Unable to extract values");
    let mut values: Vec<Box<type_system::Value>> = vec!();
    for value in json_values {
        values.push(parse_value(value));
    }
    values
}

fn parse_value(json: &serde_json::Value) -> Box<type_system::Value> {
    let type_id = TypeID::from_string(get_string(&json["type"]));
    type_id.parse(&json["value"].as_str().unwrap())
}

fn parse_limit(json: &serde_json::Value) -> Node {
    let input = parse_node(&json["input"]);
    let limit = get_int(&json["limit"]);

    let limit_operator = Limit::new(input.get_output_relation(), limit);

    Node::new(Rc::new(limit_operator), vec![Rc::new(input)])
}

fn get_string(json: &serde_json::Value) -> String {
    json.as_str().unwrap().to_string()
}

fn get_int(json: &serde_json::Value) -> u32 {
    json.as_str().unwrap().to_string().parse::<u32>().unwrap()
}

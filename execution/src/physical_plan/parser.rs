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
use message::{Plan, Function::{Eq, Lt, Le, Gt, Ge, And, Or}, Function};

fn parse(plan: Plan) -> Result<Node, String> {
    match plan {
        Plan::Aggregate { input, aggregates, groups } => parse_aggregate(input, aggregates, groups),
        Plan::CreateTable { name, columns } => parse_create_table(name, columns),
        Plan::SelectProject { input, select, project } => parse_select(input, select, project),
        Plan::TableReference { name, columns } => parse_table_reference(name, columns),
        _ => panic!("Unsupported plan type"),
    }
//
//    let json_name = json["json_name"].as_str().unwrap();
//    match json_name {
//        "TableReference" => parse_table_reference(json),
//        "Selection" => parse_selection(json),
//        "Aggregate" => parse_aggregate(json),
//        "HashJoin" | "NestedLoopsJoin" => parse_join(json),
//        "Limit" => parse_limit(json),
//        "InsertTuple" => parse_insert_tuple(json),
//        "DeleteTuples" => parse_delete_tuples(json),
//        "UpdateTable" => parse_update_table(json),
//        "CreateTable" => parse_create_table(json),
//        "DropTable" => parse_drop_table(json),
//        _ => panic!("Optimizer tree node type {} not supported", json_name),
//    }
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

fn parse_aggregate(
    input: Box<Plan>,
    aggregates: Vec<Plan>,
    groups: Vec<Plan>
) -> Result<Node, String> {
    debug_assert_eq!(aggregates.len(), 1, "Aggregating on multiple functions is not supported");
    debug_assert_eq!(groups.len(), 1, "Grouping on multiple functions is not supported");

    let input = parse(input.into())?;

    if let Plan::Function { function, arguments, output_type } = aggregates[0].into() {
        debug_assert_eq!(arguments.len(), 1,
                         "Aggregate functions with multiple arguments are not supported");

        let agg_col_in = parse_column(arguments[0].into())?;
        let agg_name = function.as_str();
        let agg_out_name = format!("{}({})", agg_name, agg_col_in.get_name());
        let agg_out_type = DataType::from_str(&output_type).unwrap();
        let agg_col_out = Column::new(&agg_out_name, agg_out_type);
    } else {

    }




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

fn parse_connective_predicate(function: Function, arguments: Vec<Plan>) -> Result<Connective, String> {
    let connective_type = ConnectiveType::from_str(function.as_str());
    let terms = arguments.into_iter().map(|t| parse_predicate(t)).collect();
    Ok(Connective::new(connective_type, terms))
}

fn parse_comparison_predicate(function: Function, arguments: Vec<Plan>) -> Result<Comparison, String> {
    debug_assert_eq!(arguments.len(), 2, "Comparison predicate function must have two arguments");

    let filter_col = parse_column(arguments[0].into())?;
    let comp_value = parse_literal(arguments[1].into())?;
    let comparator = Comparator::from_str(function.as_str())?;

    Ok(Comparison::new(filter_col, comparator, comp_value))
}

fn parse_literal(literal: Plan) -> Result<Box<types::Value>, String> {
    if let Plan::Literal { value, literal_type } = literal {
        let comp_value_type = DataType::from_str(&literal_type).unwrap();
        Ok(comp_value_type.parse(&value).unwrap())
    } else {
        Err("Invalid plan node (expected Literal)".to_string())
    }
}

fn parse_predicate(plan: Plan) -> Result<Box<Predicate>, String> {
    if let Plan::Function { function, arguments, output_type } = plan {
        match function {
            Eq | Lt | Le | Gt | Ge => Box::new(parse_comparison_predicate(function, arguments)),
            And | Or => Box::new(parse_connective_predicate(function, arguments))
        }
    } else {
        Err("Invalid plan node (expected Function)".to_string())
    }
}

fn parse_select(input: Box<Plan>, select: Option<Plan>, project: Vec<Plan>) -> Result<Node, String> {
    let input = parse(from.into())?;
    let output_cols = parse_columns(project)?;

    let project_op = filter
        .map(|predicate| Project::new(
            input.get_output_relation().unwrap(),
            output_cols,
            parse_predicate(predicate)
        ))
        .unwrap_or_else(|| Project::pure_project(
            input.get_output_relation().unwrap(),
            output_cols
        ));

    Ok(Node::new(Rc::new(project_op), vec![Rc::new(input)]))
}

fn parse_create_table(name: String, columns: Vec<Plan>) -> Result<Node, String> {
    let cols = parse_columns(columns)?;
    let relation = Relation::new(&name, Schema::new(cols));
    Ok(Node::new(Rc::new(CreateTable::new(relation)), vec![]))
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

fn parse_table_reference(name: String, columns: Vec<Plan>) -> Result<Node, String> {
    let cols = parse_columns(columns)?;
    let relation = Relation::new(&name, Schema::new(cols));
    Ok(Node::new(Rc::new(TableReference::new(relation)), vec![]))
}

fn parse_columns(columns: Vec<Plan>) -> Result<Vec<Column>, String> {
    columns.into_iter().map(|c| parse_column(c)).collect()
}

fn parse_column_names(json: &serde_json::Value) -> Vec<String> {
    let json_cols = json.as_array().expect("Unable to extract columns");
    let mut names: Vec<String> = vec![];
    for column in json_cols {
        names.push(parse_column_name(column).to_string());
    }
    names
}

fn parse_column(column: Plan) -> Result<Column, String> {
    if let Plan::Column { name, alias, column_type } = column {
        Ok(Column::new(
            &parse_column_name(name, alias),
            DataType::from_str(&column_type).unwrap(),
        ))
    } else {
        Err("Invalid plan node (expected Column)".to_string())
    }
}

fn parse_column_name(name: String, alias: Option<String>) -> String {
    alias.unwrap_or(name)
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
    let input = parse_nodeparse_node(&json["input"]);
    let limit = json["limit"].as_str().unwrap().parse::<usize>().unwrap();
    let limit_operator = Limit::new(input.get_output_relation().unwrap(), limit);
    Node::new(Rc::new(limit_operator), vec![Rc::new(input)])
}

fn get_string(json: &serde_json::Value) -> String {
    json.as_str().unwrap().to_string()
}

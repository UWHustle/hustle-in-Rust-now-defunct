extern crate message;
extern crate serde_json;

use std::rc::Rc;

use logical_entities::column::Column;
use logical_entities::predicates::comparison::*;
use logical_entities::predicates::connective::*;
use logical_entities::predicates::Predicate;
use logical_entities::relation::Relation;
use logical_entities::row::Row;
use logical_entities::schema::Schema;
use physical_operators::aggregate::Aggregate;
use physical_operators::create_table::CreateTable;
use physical_operators::delete::Delete;
use physical_operators::drop_table::DropTable;
use physical_operators::insert::Insert;
use physical_operators::join::Join;
use physical_operators::limit::Limit;
use physical_operators::select::Select;
use physical_operators::table_reference::TableReference;
use physical_operators::update::Update;
use physical_plan::node::Node;
use types;
use types::data_type::*;
use types::operators::*;

use self::message::Plan;


pub fn parse(plan: &Plan) -> Result<Node, String> {
    match plan {
        Plan::Aggregate { table, aggregates, groups } => parse_aggregate(table, aggregates, groups),
        Plan::CreateTable { name, columns } => parse_create_table(name, columns),
        Plan::Delete { from_table, filter } => parse_delete(from_table, filter),
        Plan::DropTable { table } => parse_drop_table(table),
        Plan::Insert { into_table, input } => parse_insert(into_table, input),
        Plan::Join { l_table, r_table, filter } => parse_join(l_table, r_table, filter),
        Plan::Limit { table, limit } => parse_limit(table, limit.clone()),
        Plan::Select { table, filter } => parse_select(table, filter),
        Plan::Table { name, columns } => parse_table_reference(name, columns),
        Plan::Update { table, columns, assignments, filter } =>
            parse_update(table, columns, assignments, filter),
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

fn parse_join(
    l_table: &Plan,
    r_table: &Plan,
    filter: &Option<Box<Plan>>
) -> Result<Node, String> {
    let l_node = parse(l_table)?;
    let r_node = parse(r_table)?;
    let join_op = Join::new(
        l_node.get_output_relation().unwrap(),
        r_node.get_output_relation().unwrap(),
        vec![],
        vec![],
    );
    Ok(Node::new(Rc::new(join_op), vec![Rc::new(l_node), Rc::new(r_node)]))
}

fn parse_aggregate(
    table: &Plan,
    aggregates: &Vec<Plan>,
    groups: &Vec<Plan>
) -> Result<Node, String> {
    debug_assert_eq!(aggregates.len(), 1, "Aggregating on multiple functions is not supported");
    debug_assert_eq!(groups.len(), 1, "Grouping on multiple columns is not supported");
    let input = parse(table.into())?;
    if let Plan::Function { name, arguments, output_type } = &aggregates[0] {
        debug_assert_eq!(arguments.len(), 1,
                         "Aggregate functions with multiple arguments are not supported");
        let agg_col_in = parse_column(&arguments[0])?;
        let agg_name = name.as_str();
        let agg_out_name = format!("{}({})", agg_name, agg_col_in.get_name());
        let agg_out_type = DataType::from_str(&output_type).unwrap();
        let agg_col_out = Column::new(&agg_out_name, agg_out_type);
        let output_col_names = parse_columns(groups)?
            .iter()
            .map(|col| col.get_name().to_string())
            .collect();
        let agg_op = Aggregate::from_str(
            input.get_output_relation().unwrap(),
            agg_col_in,
            agg_col_out,
            output_col_names,
            agg_name,
        )?;
        Ok(Node::new(Rc::new(agg_op), vec![Rc::new(input)]))
    } else {
        Err("Invalid plan node (expected Function)".to_string())
    }
}

fn parse_insert(into_table: &Plan, input: &Plan) -> Result<Node, String> {
    let into = parse(into_table.into())?;
    let relation = into.get_output_relation();
    let values = parse_row(input)?;
    let row = Row::new(relation.as_ref().unwrap().get_schema().clone(), values);
    let insert_op = Insert::new(relation.unwrap(), row);
    Ok(Node::new(Rc::new(insert_op), vec![Rc::new(into)]))
}

fn parse_delete(from_table: &Plan, filter: &Option<Box<Plan>>) -> Result<Node, String> {
    let from = parse(from_table.into())?;
    let filter = filter
        .as_ref()
        .map(|f| parse_filter(&f))
        .transpose()?;
    let delete_op = Delete::new(from.get_output_relation().unwrap(), filter);
    Ok(Node::new(Rc::new(delete_op), vec![Rc::new(from)]))
}

fn parse_row(row: &Plan) -> Result<Vec<Box<types::Value>>, String> {
    if let Plan::Row { values } = row {
        values.into_iter()
            .map(|literal| parse_literal(&literal))
            .collect()
    } else {
        Err("Invalid plan node (expected Row)".to_string())
    }
}

fn parse_connective(name: &str, arguments: &Vec<Plan>) -> Result<Connective, String> {
    let connective_type = ConnectiveType::from_str(name);
    let terms: Result<Vec<Box<Predicate>>, String> = arguments.into_iter()
        .map(|t| parse_filter(&t))
        .collect();
    Ok(Connective::new(connective_type, terms?))
}

fn parse_comparative(name: &str, arguments: &Vec<Plan>) -> Result<Comparison, String> {
    debug_assert_eq!(arguments.len(), 2, "Comparison predicate function must have two arguments");

    let filter_col = parse_column(&arguments[0])?;
    let comp_value = parse_literal(&arguments[1])?;
    let comparator = Comparator::from_str(name)?;

    Ok(Comparison::new(filter_col, comparator, comp_value))
}

fn parse_literal(literal: &Plan) -> Result<Box<types::Value>, String> {
    if let Plan::Literal { value, literal_type } = literal {
        let data_type = DataType::from_str(&literal_type).unwrap();
        Ok(data_type.parse(&value).unwrap())
    } else {
        Err("Invalid plan node (expected Literal)".to_string())
    }
}

fn parse_filter(plan: &Plan) -> Result<Box<Predicate>, String> {
    if let Plan::Function { name, arguments, output_type } = plan {
        match name.as_str() {
            "eq" | "lt" | "le" | "gt" | "ge" =>
                Ok(Box::new(parse_comparative(name, arguments)?)),
            "and" | "or" => Ok(Box::new(parse_connective(name, arguments)?)),
            _ => Err(format!("Invalid function name {}", name))
        }
    } else {
        Err("Invalid plan node (expected Function)".to_string())
    }
}

fn parse_select(table: &Plan, filter: &Plan) -> Result<Node, String> {
    let input = parse(table.into())?;

    let select_op = Select::new(
        input.get_output_relation().unwrap(),
        parse_filter(filter)?
    );

    Ok(Node::new(Rc::new(select_op), vec![Rc::new(input)]))
}

fn parse_create_table(name: &str, columns: &Vec<Plan>) -> Result<Node, String> {
    let cols = parse_columns(columns.to_owned())?;
    let relation = Relation::new(name, Schema::new(cols));
    Ok(Node::new(Rc::new(CreateTable::new(relation)), vec![]))
}

fn parse_update(
    table: &Plan,
    columns: &Vec<Plan>,
    assignments: &Vec<Plan>,
    filter: &Option<Box<Plan>>,
) -> Result<Node, String> {
    let input = parse(table.into())?;
    let columns = parse_columns(columns)?;
    let assignments: Result<Vec<Box<types::Value>>, String> = assignments.into_iter()
        .map(|assignment| parse_literal(assignment))
        .collect();
    let filter = filter
        .as_ref()
        .map(|f| parse_filter(&f))
        .transpose()?;
    let update_op = Update::new(
        input.get_output_relation().unwrap(),
        filter,
        columns,
        assignments?
    );
    Ok(Node::new(Rc::new(update_op), vec![Rc::new(input)]))
}

fn parse_drop_table(table: &Plan) -> Result<Node, String> {
    if let Plan::Table { name, columns } = table {
        Ok(Node::new(Rc::new(DropTable::new(&name)), vec![]))
    } else {
        Err("Invalid plan node (expected TableReference)".to_string())
    }
}

fn parse_table_reference(name: &str, columns: &Vec<Plan>) -> Result<Node, String> {
    let cols = parse_columns(columns)?;
    let relation = Relation::new(&name, Schema::new(cols));
    Ok(Node::new(Rc::new(TableReference::new(relation)), vec![]))
}

fn parse_columns(columns: &Vec<Plan>) -> Result<Vec<Column>, String> {
    columns.into_iter().map(|c| parse_column(c)).collect()
}

fn parse_column(column: &Plan) -> Result<Column, String> {
    if let Plan::Column { name, column_type, table, alias } = column {
        Ok(Column::new(
            alias.as_ref().map(|a| a.as_str()).unwrap_or(name),
            DataType::from_str(&column_type).unwrap(),
        ))
    } else {
        Err("Invalid plan node (expected Column)".to_string())
    }
}

fn parse_limit(table: &Plan, limit: usize) -> Result<Node, String> {
    let input = parse(table.into())?;
    let limit_operator = Limit::new(input.get_output_relation().unwrap(), limit);
    Ok(Node::new(Rc::new(limit_operator), vec![Rc::new(input)]))
}

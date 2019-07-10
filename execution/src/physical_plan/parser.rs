extern crate hustle_common;
extern crate serde_json;

use std::rc::Rc;

use logical_entities::column::Column;
use logical_entities::predicates::comparison::*;
use logical_entities::predicates::connective::*;
use logical_entities::predicates::Predicate;
use logical_entities::relation::Relation;
use logical_entities::row::Row;
use logical_entities::schema::Schema;
use physical_operators::create_table::CreateTable;
use physical_operators::delete::Delete;
use physical_operators::drop_table::DropTable;
use physical_operators::insert::Insert;
use physical_operators::join::Join;
use physical_operators::limit::Limit;
use physical_operators::project::Project;
use physical_operators::select::Select;
use physical_operators::table_reference::TableReference;
use physical_operators::update::Update;
use physical_plan::node::Node;
use hustle_types;
use hustle_types::data_type::*;
use hustle_types::operators::*;

use self::hustle_common::{Plan, Table};


pub fn parse(plan: &Plan) -> Result<Node, String> {
    match plan {
        Plan::Aggregate { table, aggregates, groups } => parse_aggregate(table, aggregates, groups),
        Plan::CreateTable { table } => parse_create_table(table),
        Plan::Delete { from_table, filter } => parse_delete(from_table, filter),
        Plan::DropTable { table } => parse_drop_table(table),
        Plan::Insert { into_table, input } => parse_insert(into_table, input),
        Plan::Join { l_table, r_table, filter } => parse_join(l_table, r_table, filter),
        Plan::Limit { table, limit } => parse_limit(table, limit.clone()),
        Plan::Project { table, projection } => parse_project(table, projection),
        Plan::Select { table, filter } => parse_select(table, filter),
        Plan::TableReference { table } => Ok(parse_table(table)),
        Plan::Update { table, columns, assignments, filter } =>
            parse_update(table, columns, assignments, filter),
        _ => panic!("Unsupported plan type"),
    }
}

fn parse_join(
    l_table: &Plan,
    r_table: &Plan,
    _filter: &Option<Box<Plan>>
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
    _table: &Plan,
    _aggregates: &Vec<Plan>,
    _groups: &Vec<Plan>
) -> Result<Node, String> {
    Err("Aggregate functions are not yet supported in the execution engine parser".to_string())
}

fn parse_insert(into_table: &Table, input: &Plan) -> Result<Node, String> {
    let into = parse_table(into_table.into());
    let relation = into.get_output_relation();
    let values = parse_row(input)?;
    let row = Row::new(relation.as_ref().unwrap().get_schema().clone(), values);
    let insert_op = Insert::new(relation.unwrap(), row);
    Ok(Node::new(Rc::new(insert_op), vec![Rc::new(into)]))
}

fn parse_delete(from_table: &Table, filter: &Option<Box<Plan>>) -> Result<Node, String> {
    let from = parse_table(from_table.into());
    let filter = filter
        .as_ref()
        .map(|f| parse_filter(&f))
        .transpose()?;
    let delete_op = Delete::new(from.get_output_relation().unwrap(), filter);
    Ok(Node::new(Rc::new(delete_op), vec![Rc::new(from)]))
}

fn parse_row(row: &Plan) -> Result<Vec<Box<hustle_types::Value>>, String> {
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

fn parse_comparative(name: &str, left: &Box<Plan>, right: &Box<Plan>) -> Result<Comparison, String> {
    let comparator = Comparator::from_str(name)?;
    let l_operand = parse_column_reference(&left)?;
    match &**right {
        Plan::Literal { value: _, literal_type: _ } => {
            let r_operand = parse_literal(right)?;
            Ok(Comparison::new(comparator, l_operand, ComparisonOperand::Value(r_operand)))
        },
        Plan::ColumnReference { column: c } => {
            let r_operand = parse_column(c);
            Ok(Comparison::new(comparator, l_operand, ComparisonOperand::Column(r_operand)))
        },
        _ => Err("Invalid plan node (expected Literal or ColumnReference".to_string())
    }
}

fn parse_literal(literal: &Plan) -> Result<Box<hustle_types::Value>, String> {
    if let Plan::Literal { value, literal_type } = literal {
        let data_type = DataType::from_str(&literal_type).unwrap();
        Ok(data_type.parse(&value).unwrap())
    } else {
        Err("Invalid plan node (expected Literal)".to_string())
    }
}

fn parse_filter(plan: &Plan) -> Result<Box<Predicate>, String> {
    match plan {
        Plan::Comparative { name, left, right } =>
            Ok(Box::new(parse_comparative(name, left, right)?)),
        Plan::Connective { name, terms } =>
            Ok(Box::new(parse_connective(name, terms)?)),
        _ => Err("Invalid plan node (expected Comparative or Connective)".to_string())
    }
}

fn parse_project(table: &Plan, projection: &Vec<Plan>) -> Result<Node, String> {
    let input = parse(table.into())?;
    let project_op = Project::new(
        input.get_output_relation().unwrap(),
        parse_column_references(projection)?
    );
    Ok(Node::new(Rc::new(project_op), vec![Rc::new(input)]))
}

fn parse_select(table: &Plan, filter: &Plan) -> Result<Node, String> {
    let input = parse(table.into())?;
    let select_op = Select::new(
        input.get_output_relation().unwrap(),
        parse_filter(filter)?
    );
    Ok(Node::new(Rc::new(select_op), vec![Rc::new(input)]))
}

fn parse_create_table(table: &Table) -> Result<Node, String> {
    let cols = parse_columns(&table.columns);
    let relation = Relation::new(&table.name, Schema::new(cols));
    Ok(Node::new(Rc::new(CreateTable::new(relation)), vec![]))
}

fn parse_update(
    table: &Table,
    columns: &Vec<hustle_common::Column>,
    assignments: &Vec<Plan>,
    filter: &Option<Box<Plan>>,
) -> Result<Node, String> {
    let input = parse_table(table.into());
    let columns = parse_columns(columns);
    let assignments: Result<Vec<Box<hustle_types::Value>>, String> = assignments.into_iter()
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

fn parse_drop_table(table: &Table) -> Result<Node, String> {
    Ok(Node::new(Rc::new(DropTable::new(&table.name)), vec![]))
}

fn parse_column_references(column_references: &Vec<Plan>) -> Result<Vec<Column>, String> {
    column_references.iter()
        .map(|c| parse_column_reference(c))
        .collect()
}

fn parse_column_reference(column_reference: &Plan) -> Result<Column, String> {
    if let Plan::ColumnReference { column } = column_reference {
        Ok(parse_column(column))
    } else {
        Err("Invalid plan node type (expected ColumnReference)".to_string())
    }
}

fn parse_columns(columns: &Vec<hustle_common::Column>) -> Vec<Column> {
    columns.into_iter().map(|c| parse_column(c)).collect()
}

fn parse_column(column: &hustle_common::Column) -> Column {
    Column::new(
        &column.name,
        DataType::from_str(&column.column_type).unwrap(),
    )
}

fn parse_table(table: &hustle_common::Table) -> Node {
    let cols = parse_columns(&table.columns);
    let relation = Relation::new(&table.name, Schema::new(cols));
    Node::new(Rc::new(TableReference::new(relation)), vec![])
}

fn parse_limit(table: &Plan, limit: usize) -> Result<Node, String> {
    let input = parse(table.into())?;
    let limit_operator = Limit::new(input.get_output_relation().unwrap(), limit);
    Ok(Node::new(Rc::new(limit_operator), vec![Rc::new(input)]))
}

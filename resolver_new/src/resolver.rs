use hustle_common::{Plan, Column, Table, Expression, ComparativeVariant, QueryOperator, Query as CommonQuery, AggregateFunction, AggregateFunctionVariant, ConnectiveVariant};
//use hustle_common::Message;
//use hustle_common::*;
//use serde_json;
//use std::sync::mpsc::{Receiver, Sender};
use hustle_resolver::catalog::Catalog;
//use sqlparser::ast::Statement;
//use sqlparser::ast::TransactionMode;
use sqlparser::ast::*;
use std::result::Result::Err;
//use sqlparser::ast::Statement::{SetTransaction, Commit};
//use hustle_common::Message::ExecutePlan;


pub struct Resolver {
    pub catalog: Catalog
}

impl Resolver {
    pub fn new() -> Self {
        Resolver::with_catalog(Catalog::try_from_file().unwrap_or(Catalog::new()))
    }

    pub fn with_catalog(catalog: Catalog) -> Self {
        Resolver {
            catalog
        }
    }

    /*pub fn listen(
        &mut self,
        resolver_rx: Receiver<Vec<u8>>,
        transaction_tx: Sender<Vec<u8>>,
        completed_tx: Sender<Vec<u8>>,
    ) {
        loop {
            let buf = resolver_rx.recv().unwrap();
            let request = Message::deserialize(&buf).unwrap();
            match request {
                Message::ResolveAst { ast, connection_id } => {
                    match self.resolve(&ast[0]) {
                        Ok(plan) => transaction_tx.send(Message::TransactPlan {
                            plan,
                            connection_id,
                        }.serialize().unwrap()).unwrap(),
                        Err(reason) => completed_tx.send(Message::Failure {
                            reason,
                            connection_id,
                        }.serialize().unwrap()).unwrap()
                    }
                },
                _ => completed_tx.send(buf).unwrap()
            };
        }
    }*/

    pub fn resolve(&mut self, ast: Statement) -> Result<Plan, String> {
        /*serde_json::from_str(ast)
            .map_err(|e| e.to_string())
            .and_then(|node| self.resolve_node(&node))*/

        match ast {
            Statement::Query(q) => self.resolve_query(q),
            Statement::Insert {table_name, columns, source} => self.resolve_insert(table_name, columns, source),
            Statement::Update {table_name, assignments, selection} => self.resolve_update(table_name, assignments, selection),
            Statement::Delete {table_name, selection} => self.resolve_delete(table_name, selection),
            Statement::CreateTable { name, columns, constraints: _, with_options: _, external: _, file_format: _, location: _} => self.resolve_create_table(name, columns),
            Statement::Drop { object_type, if_exists, names, cascade} => self.resolve_drop(object_type, if_exists, names, cascade),
            Statement::StartTransaction { modes } => self.resolve_start_transaction(modes),
            Statement::Commit { chain } => self.resolve_commit(chain),
            _ => Err(format!("Unrecognized AST type {}", ast))
        }
    }

    fn resolve_query(&self, q: Box<Query>) -> Result<Plan, String> {
        let mut query = match q.body {
            SetExpr::Select(s) => {
                let mut query = self.resolve_input(&s.from)?;
                if let Some(s) = s.selection {
                    query = self.resolve_select(query, s)?;
                }
                if s.group_by.len() != 0 {
                    let functions: Vec<Function> = s.projection.iter().cloned().filter_map(|s|
                        if let SelectItem::UnnamedExpr(e) = s {
                            if let Expr::Function(f) = e {
                                let func_name = f.to_string().to_lowercase();
                                match func_name.as_ref() {
                                    "avg" | "count" | "min" | "max" | "sum" => Some(f),
                                    _ => panic!(format!("Unrecognized Function type {}", func_name))
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    ).collect();
                    query = self.resolve_aggregate(query, &s.group_by, functions)?;
                }
                query = self.resolve_project(query, &s.projection)?;
                query
            },
            _ => panic!(format!("Unsupported SetExpr type {}", q.body))
        };

        if q.limit.is_some() {
            query = self.resolve_limit(query, q.limit.unwrap())?;
        }

        Ok(Plan::Query {query})
    }

    fn resolve_input(&self, from: &Vec<TableWithJoins>) -> Result<CommonQuery, String> {
        if from.len() == 1 {
            let relation = &from[0].relation;
            let table = match relation {
                TableFactor::Table {name, alias: _, args: _, with_hints: _} => self.resolve_table(&name.to_string())?,
                _ => panic!(format!("Unsupported TableFactor type {}", relation))
            };
            Ok(CommonQuery {
                output: table.columns.clone(),
                operator: QueryOperator::TableReference { table }
            })
        } else {
            let tables: Result<Vec<CommonQuery>, String> = from.iter()
                .map(|t| {
                    let table = match &t.relation {
                        TableFactor::Table { name, alias: _, args: _, with_hints: _ } => self.resolve_table(&name.to_string())?,
                        _ => panic!(format!("Unsupported TableFactor type {}", t.relation))
                    };

//                    for c in table.columns.iter() {
//                        output.push(Column::new(
//                            format!("{}.{}", table.name, c.name),
//                            c.column_type.clone(),
//                            table.name.clone(),
//                        ));
//                    }

                    Ok(CommonQuery {
                        output: table.columns.clone(),
                        operator: QueryOperator::TableReference { table },
                    })
                }).collect();

            let tables = tables?;
            let mut output = vec![];
            for q in tables.iter() {
                output.extend(q.output.clone());
            }

            Ok(CommonQuery {
                operator: QueryOperator::Join {
                    input: tables,
                    filter: None,
                },
                output,
            })
        }
    }

    fn resolve_select(&self, input: CommonQuery, s: Expr) -> Result<CommonQuery, String> {
        let active_columns = input.output.clone();
        let expr = self.resolve_expr(&s, &vec![&active_columns])?;
        Ok(CommonQuery {
            operator: QueryOperator::Select {
                input: Box::new(input),
                filter: Box::new(expr)
            },
            output: active_columns
        })
    }

    fn resolve_aggregate(&self, input: CommonQuery, group_by: &Vec<Expr>, agg: Vec<Function>) -> Result<CommonQuery, String> {
        let mut active_columns = input.output.clone();

        let groups: Result<Vec<Expression>, String>  = group_by.iter()
            .map(|e| self.resolve_expr(e, &vec![&active_columns]))
            .collect();

        let mut aggregate_columns = vec![];

        let aggregates: Vec<AggregateFunction> = agg.iter().map(|f| {
            let func_name = f.name.to_string().to_lowercase();
            let column = active_columns.iter()
                .find(|c| c.name.eq(&format!("{}", f.args[0])))
                .cloned().unwrap();

            aggregate_columns.push(Column::new(
                format!("{}", f),
                column.column_type.clone(),
                "".to_string()
            ));

            match func_name.as_ref() {
                "avg" => AggregateFunction { variant: AggregateFunctionVariant::Avg, column },
                "count" => AggregateFunction { variant: AggregateFunctionVariant::Count, column },
                "max" => AggregateFunction { variant: AggregateFunctionVariant::Max, column },
                "min" => AggregateFunction { variant: AggregateFunctionVariant::Min, column },
                "sum" => AggregateFunction { variant: AggregateFunctionVariant::Sum, column },
                _ => panic!(format!("Unsupported Function type {}", func_name))
            }
        }).collect();

        aggregate_columns.append(&mut active_columns);

        Ok(CommonQuery {
            operator: QueryOperator::Aggregate { input: Box::new(input), aggregates, groups: groups? },
            output: aggregate_columns
        })
    }

    fn resolve_project(&self, input: CommonQuery, p: &Vec<SelectItem>) -> Result<CommonQuery, String> {
        let active_columns = vec![&input.output];
        let mut output = vec![];
        for item in p {
            let expr = match item {
                SelectItem::UnnamedExpr(e) => self.resolve_expr(e, &active_columns)?,
                _ => panic!(format!("Unrecognized SelectItem type {}", item))
            };
            match expr {
                Expression::ColumnReference {table, column} => output.push(active_columns[table][column].clone()),
                _ => panic!(format!("Unrecognized SelectItem type {}", item))
            }
        }
        Ok(CommonQuery {
            operator: QueryOperator::Project { input: Box::new(input) },
            output
        })
    }

    fn resolve_limit(&self, input: CommonQuery, limit_expr: Expr) -> Result<CommonQuery, String> {
        let active_columns = input.output.clone();
        let limit = match limit_expr {
            Expr::Value(v) => v.to_string().parse::<usize>().expect("Limit value not being a number"),
            _ => panic!(format!("Unrecognized limit type {}", limit_expr))
        };
        Ok(CommonQuery {
            operator: QueryOperator::Limit {input: Box::new(input), limit},
            output: active_columns
        })
    }

    fn resolve_insert(&self, table_name: ObjectName, _columns: Vec<String>, source: Box<Query>) -> Result<Plan, String> {
        let into_table = self.resolve_table(&table_name.to_string())?;
        match source.body {
            SetExpr::Values(value) => {
                let values: Result<Vec<Expression>, String> = value.0[0]
                    .iter()
                    .map(|e| self.resolve_expr(e, &vec![&into_table.columns]))
                    .collect();
                Ok(Plan::Insert {into_table, values: values?})
            },
            _ => Err(format!("Unrecognized insert type {}", source.body))
        }
    }

    fn resolve_update(&self, table_name: ObjectName, assignments: Vec<Assignment>, selection: Option<Expr>) -> Result<Plan, String> {
        let table = self.resolve_table(&table_name.to_string())?;
        let columns: Result<Vec<Column>, String> = assignments.iter()
            .map(|a| self.resolve_column(&table, &a.id))
            .collect();

        let columns = columns?;
        //let active_columns = vec![columns.clone()];
        let assignments: Result<Vec<Expression>, String> = assignments.iter()
            .map(|a| self.resolve_expr(&a.value, &vec![&columns]))
            .collect();

        let filter = match selection {
            Some(t) => Some(Box::new(
                self.resolve_expr(&t, &vec![&columns])?
            )),
            None => None
        };
        Ok(Plan::Update { table, columns, assignments: assignments?, filter })
    }

    fn resolve_delete(&self, table_name: ObjectName, selection: Option<Expr>) -> Result<Plan, String> {
        let from_table = self.resolve_table(&table_name.to_string())?;
        //let active_columns = vec![from_table.columns.clone()];
        let filter = self.resolve_expr(&selection.unwrap(), &vec![&from_table.columns])?;
        Ok(Plan::Delete {
            from_table,
            filter: Some(Box::new(filter))
        })
    }

    fn resolve_create_table(&mut self, name: ObjectName, columns: Vec<ColumnDef>) -> Result<Plan, String> {
        let name= name.to_string();
        if self.catalog.table_exists(&name) {
            Err(format!("Table {} already exists", &name))
        } else {
            let columns = columns.iter().map(|column| {
                    Column::new(
                        column.name.clone(),
                        column.data_type.to_string(),
                        name.clone(),
                    )
                }).collect();
            let table = Table::new(name, columns);
            self.catalog.create_table(table.clone())?;
            Ok(Plan::CreateTable { table })
        }
    }

    fn resolve_drop(&mut self, object_type: ObjectType, if_exists: bool, names: Vec<ObjectName>, _cascade: bool) -> Result<Plan, String> {
        assert_eq!(object_type, ObjectType::Table);
        assert_eq!(if_exists, false);
        assert_eq!(names.len(), 1);

        let table = self.resolve_table(&names[0].to_string())?;
        self.catalog.drop_table(&table.name)?;
        Ok(Plan::DropTable { table })
    }

    fn resolve_start_transaction(&self, _modes: Vec<TransactionMode>) -> Result<Plan, String> {
        Ok(Plan::BeginTransaction)
    }

    fn resolve_commit(&self, _chain: bool) -> Result<Plan, String> {
        Ok(Plan::CommitTransaction)
    }

    fn resolve_table(&self, name: &str) -> Result<Table, String> {
        self.catalog.get_table(name)
            .map(|table| table.clone())
            .ok_or(format!("Unrecognized table: {}", name))
    }

    fn resolve_column(&self, table: &Table, column_name: &str) -> Result<Column, String> {
        table.columns.iter()
            .find(|c| c.name.eq(column_name))
            .map(|c| c.clone())
            .ok_or(format!("Unrecognized column: {}", column_name))
    }

    fn resolve_expr(&self, expr: &Expr, active_columns: &Vec<&Vec<Column>>) -> Result<Expression, String> {
        match expr {
            Expr::Between {expr, negated, low, high} => {
                let column = self.resolve_expr(expr, active_columns)?;
                let low = self.resolve_expr(low, active_columns)?;
                let high = self.resolve_expr(high, active_columns)?;
                let (var1, var2, var3) = match negated {
                    true => (ConnectiveVariant::Or, ComparativeVariant::Lt, ComparativeVariant::Gt),
                    false => (ConnectiveVariant::And, ComparativeVariant::Ge, ComparativeVariant::Le),
                };
                Ok(Expression::Connective {
                    variant: var1,
                    terms: vec![
                        Expression::Comparative {
                            variant: var2,
                            left: Box::new(column.clone()),
                            right: Box::new(low)
                        },
                        Expression::Comparative {
                            variant: var3,
                            left: Box::new(column),
                            right: Box::new(high)
                        }
                    ]
                })
            }
            Expr::BinaryOp {left, op, right} => {
                let left_plan = self.resolve_expr(left, active_columns)?;
                let right_plan = self.resolve_expr(right, active_columns)?;

                match op {
                    BinaryOperator::Eq | BinaryOperator::Lt | BinaryOperator::LtEq | BinaryOperator::Gt | BinaryOperator::GtEq => {
                        let variant = match op {
                            BinaryOperator::Eq => ComparativeVariant::Eq,
                            BinaryOperator::Lt => ComparativeVariant::Lt,
                            BinaryOperator::LtEq => ComparativeVariant::Le,
                            BinaryOperator::Gt => ComparativeVariant::Gt,
                            BinaryOperator::GtEq => ComparativeVariant::Ge,
                            _ => panic!(format!("Unsupported BinaryOperator Variant {}", op))
                        };
                        Ok(Expression::Comparative {
                            variant,
                            left: Box::new(left_plan),
                            right: Box::new(right_plan)
                        })
                    }
                    BinaryOperator::And | BinaryOperator::Or => {
                        let variant = match op {
                            BinaryOperator::And => ConnectiveVariant::And,
                            BinaryOperator::Or => ConnectiveVariant::Or,
                            _ => panic!(format!("Unsupported BinaryOperator Variant {}", op))
                        };
                        Ok(Expression::Connective {
                            variant,
                            terms: vec![left_plan, right_plan]
                        })
                    }
                    _ => Err(format!("Unsupported BinaryOperator Variant {}", op))
                }
            }
            Expr::Identifier(_column_name) => {
                let mut res = None;
                for i in 0..active_columns.len() {
                    let columns = active_columns[i];
                    for j in 0..columns.len() {
                        if columns[j].name.eq(&format!("{}", expr)) {
                            res = Some((i, j));
                        }
                    }
                };
                match res {
                    Some((table, column)) => Ok(Expression::ColumnReference { table, column }),
                    None => Err(format!("Identifier {} not found", expr))
                }
            }
            Expr::CompoundIdentifier(column_names) => {
                assert_eq!(column_names.len(), 2, "Invalid CompoundIdentifier {}", expr);
                let mut res = None;
                for i in 0..active_columns.len() {
                    let columns = active_columns[i];
                    for j in 0..columns.len() {
                        if format!("{}.{}", columns[j].from_table, columns[j].name).eq(&format!("{}", expr)) {
                            res = Some((i, j));
                        }
                    }
                };
                match res {
                    Some((table, column)) => Ok(Expression::ColumnReference { table, column }),
                    None => Err(format!("CompoundIdentifier {} not found", expr))
                }
            }
            Expr::Value(value) => {
                match value {
                    Value::Long(v) => Ok(Expression::Literal {
                        value: v.to_string(),
                        literal_type: "Long".to_string(),
                    }),
                    Value::Double(v) => Ok(Expression::Literal {
                        value: v.to_string(),
                        literal_type: "Double".to_string(),
                    }),
                    Value::SingleQuotedString(v) => Ok(Expression::Literal {
                        value: v.to_string(),
                        literal_type: "SingleQuotedString".to_string(),
                    }),
                    Value::Boolean(v) => Ok(Expression::Literal {
                        value: v.to_string(),
                        literal_type: "Boolean".to_string(),
                    }),
                    _ => Err(format!("Unsupported value type {}", value))
                }
            }
//            Expr::Function(f) => Ok(Expression::Function {
//                name: format!("{}", f.name),
//                arguments: f.args.iter()
//                    .map(|arg| self.resolve_expr(arg, active_columns))
//                    .collect(),
//                output_type: "unknown_output_type".to_string()
//            }),
            _ => Err(format!("Unsupported Expr type {}", expr)),
        }
    }
}

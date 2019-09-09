use std::convert::TryFrom;
use std::result::Result::Err;
use std::sync::Arc;

use sqlparser::ast::{Assignment, BinaryOperator, ColumnDef, DataType, Expr, ObjectName, ObjectType, Query, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, TransactionMode, Value};

use hustle_catalog::{Catalog, Column, Table};
use hustle_common::plan::{Expression, Plan, Query as QueryPlan, QueryOperator};
use hustle_types::{Bool, Char, Int64, TypeVariant, ComparativeVariant};

pub struct Resolver {
    catalog: Arc<Catalog>,
}

impl Resolver {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Resolver {
            catalog,
        }
    }

    pub fn resolve(&mut self, stmts: &[Statement]) -> Result<Plan, String> {
        if stmts.is_empty() {
            Err("No statements were provided to the resolver".to_owned())
        } else if stmts.len() > 1 {
            Err("Nested queries are not yet supported".to_owned())
        } else {
            match &stmts[0] {
                Statement::Query(q) => self.resolve_query(&*q),
                Statement::Insert {
                    table_name,
                    columns: _,
                    source,
                } => self.resolve_insert(&table_name, source),
                Statement::Update {
                    table_name,
                    assignments,
                    selection,
                } => self.resolve_update(table_name, assignments, selection),
                Statement::Delete {
                    table_name,
                    selection,
                } => self.resolve_delete(table_name, selection),
                Statement::CreateTable {
                    name,
                    columns,
                    constraints: _,
                    with_options: _,
                    external: _,
                    file_format: _, location: _,
                } => self.resolve_create_table(name, columns),
                Statement::Drop {
                    object_type,
                    if_exists,
                    names,
                    cascade,
                } => self.resolve_drop(object_type, *if_exists, names, *cascade),
                Statement::StartTransaction { modes} => self.resolve_start_transaction(modes),
                Statement::Commit { chain } => self.resolve_commit(*chain),
                _ => Err(format!("Unrecognized AST type {}", stmts[0]))
            }
        }

    }

    fn resolve_query(&self, query: &Query) -> Result<Plan, String> {
        if query.limit.is_some() {
            return Err("Limit is not yet supported".to_owned());
        }

        let query_plan = match &query.body {
            SetExpr::Select(select) => {
                if select.distinct {
                    Err("Select distinct is not yet supported".to_owned())
                } else if select.having.is_some() {
                    Err("Select having is not yet supported".to_owned())
                } else if !select.group_by.is_empty() {
                    Err("Group by is not yet supported".to_owned())
                } else {
                    // Resolve the tables referenced by the select statement.
                    let mut query_plan = self.resolve_input(&select.from)?;

                    // Resolve the selection predicate of the select statement.
                    if let Some(s) = &select.selection {
                        query_plan = self.resolve_selection(query_plan, s)?;
                    }

                    // Resolve the projection of the select statement.
                    self.resolve_projection(query_plan, &select.projection)
                }
            },
            _ => Err(format!("Unsupported query type {}", query.body)),
        }?;

        Ok(Plan::Query(query_plan))
    }

    fn resolve_input(&self, from: &[TableWithJoins]) -> Result<QueryPlan, String> {
        if from.len() == 1 {
            let relation = &from.first().unwrap().relation;
            let table = match relation {
                TableFactor::Table {
                    name,
                    alias: _,
                    args: _,
                    with_hints: _,
                } => self.resolve_table(&name.to_string()),
                _ => Err(format!("Unsupported input type {}", relation))
            }?;

            Ok(QueryPlan {
                output: table.columns.clone(),
                operator: QueryOperator::TableReference(table)
            })
        } else {
            let tables = from.iter()
                .map(|t| {
                    let table = match &t.relation {
                        TableFactor::Table {
                            name,
                            alias: _,
                            args: _,
                            with_hints: _,
                        } => self.resolve_table(&name.to_string()),
                        _ => Err(format!("Unsupported input type {}", t.relation))
                    }?;

                    Ok(QueryPlan {
                        output: table.columns.clone(),
                        operator: QueryOperator::TableReference(table),
                    })
                }).collect::<Result<Vec<QueryPlan>, String>>()?;

            let output = tables.iter()
                .flat_map(|table| table.output.iter().cloned())
                .collect::<Vec<Column>>();

            Ok(QueryPlan {
                operator: QueryOperator::Cartesian {
                    inputs: tables,
                },
                output,
            })
        }
    }

    fn resolve_selection(&self, input: QueryPlan, expr: &Expr) -> Result<QueryPlan, String> {
        let active_columns = input.output.clone();
        let filter = self.resolve_expr(expr, &active_columns)?;

        Ok(QueryPlan {
            operator: QueryOperator::Select {
                input: Box::new(input),
                filter: Box::new(filter)
            },
            output: active_columns
        })
    }

    fn resolve_projection(&self, input: QueryPlan, projection: &[SelectItem]) -> Result<QueryPlan, String> {
        let active_columns = &input.output;

        let columns = projection.iter()
            .map(|select_item| {
                let expression = match select_item {
                    SelectItem::UnnamedExpr(e) => self.resolve_expr(e, &active_columns),
                    _ => Err(format!("Unrecognized SelectItem type {}", select_item))
                }?;
                match expression {
                    Expression::ColumnReference(column) => {
                        Ok(column)
                    },
                    _ => Err(format!("Unrecognized SelectItem type {}", select_item)),
                }
            })
            .collect::<Result<Vec<_>, String>>()?;

        let output = columns.iter()
            .map(|&column| active_columns[column].clone())
            .collect::<Vec<_>>();

        Ok(QueryPlan {
            operator: QueryOperator::Project {
                input: Box::new(input),
                cols: columns,
            },
            output
        })
    }

    fn resolve_insert(
        &self,
        table_name: &ObjectName,
        source: &Query
    ) -> Result<Plan, String> {
        let into_table = self.resolve_table(&table_name.to_string())?;

        match &source.body {
            SetExpr::Values(values) => {
                let values_vec = &values.0;
                if values_vec.len() == 1 {
                    let values = values_vec.first().unwrap();
                    if values.len() == into_table.columns.len() {
                        let bufs = values.iter()
                            .map(|expr| self.resolve_literal(expr))
                            .collect::<Result<Vec<_>, String>>()?;

                        Ok(Plan::Insert { into_table, bufs })
                    } else {
                        Err(format!(
                            "Wrong number of insert arguments (expected {}, found {})",
                            into_table.columns.len(),
                            values.len(),
                        ))
                    }
                } else {
                    Err("Can only insert one set of values at a time".to_owned())
                }
            },
            _ => Err(format!("Unsupported insert type {}", source.body))
        }
    }

    fn resolve_update(
        &self,
        table_name: &ObjectName,
        assignments: &[Assignment],
        selection: &Option<Expr>
    ) -> Result<Plan, String> {
        let table = self.resolve_table(&table_name.to_string())?;

        let assignments = assignments.iter()
            .map(|assignment| {
                self.resolve_column(&table, &assignment.id).and_then(|column|
                    self.resolve_literal(&assignment.value)
                        .map_err(|_| "Assignment to column must be a literal value".to_owned())
                        .map(|buf| (column, buf))
                )
            })
            .collect::<Result<Vec<_>, String>>()?;

        let filter = selection
            .as_ref()
            .map(|expr| self.resolve_expr(&expr, &table.columns))
            .transpose()?
            .map(|filter| Box::new(filter));

        Ok(Plan::Update {
            table,
            assignments,
            filter,
        })
    }

    fn resolve_delete(
        &self,
        table_name: &ObjectName,
        selection: &Option<Expr>,
    ) -> Result<Plan, String> {
        let from_table = self.resolve_table(&table_name.to_string())?;
        let filter = selection
            .as_ref()
            .map(|expr| self.resolve_expr(&expr, &from_table.columns))
            .transpose()?
            .map(|filter| Box::new(filter));

        Ok(Plan::Delete {
            from_table,
            filter
        })
    }

    fn resolve_create_table(
        &mut self,
        name: &ObjectName,
        columns: &[ColumnDef],
    ) -> Result<Plan, String> {
        let name= name.to_string();
        if self.catalog.table_exists(&name) {
            Err(format!("Table {} already exists", &name))
        } else {
            let columns = columns.iter().map(|column| {
                let type_variant = match column.data_type {
                    DataType::Boolean => Ok(TypeVariant::Bool(Bool)),
                    DataType::Int => Ok(TypeVariant::Int64(Int64)),
                    DataType::Char(len) => {
                        if let Some(len) = len {
                            Ok(TypeVariant::Char(Char::new(len as usize)))
                        } else {
                            Err("Char type must have defined length".to_owned())
                        }
                    },
                    _ => Err(format!("Unsupported data type {}", column.data_type)),
                }?;

                Ok(Column::new(
                    column.name.clone(),
                    name.to_string(),
                    type_variant,
                    false,
                ))
            }).collect::<Result<Vec<_>, String>>()?;

            let table = Table::new(name, columns);
            self.catalog.create_table(table.clone())?;
            Ok(Plan::CreateTable(table))
        }
    }

    fn resolve_drop(
        &self,
        object_type: &ObjectType,
        if_exists: bool,
        names: &[ObjectName],
        cascade: bool
    ) -> Result<Plan, String> {
        if object_type == &ObjectType::View {
            Err("Views are not yet supported".to_owned())
        } else if if_exists {
            Err("If exists is not yet supported".to_owned())
        } else if cascade {
            Err("Cascading drop is not yet supported".to_owned())
        } else if names.len() != 1 {
            Err("Cannot drop more than one table at a time".to_owned())
        } else {
            let table = self.resolve_table(&names[0].to_string())?;
            Ok(Plan::DropTable(table))
        }
    }


    fn resolve_start_transaction(&self, _modes: &[TransactionMode]) -> Result<Plan, String> {
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

    fn resolve_column(&self, table: &Table, column_name: &str) -> Result<usize, String> {
        table.columns.iter()
            .position(|column| column.get_name() == column_name)
            .ok_or(format!("Unrecognized column: {}", column_name))
    }

    fn resolve_expr(&self, expr: &Expr, active_columns: &[Column]) -> Result<Expression, String> {
        match expr {
            Expr::Between {expr, negated, low, high} => {
                let column = self.resolve_expr(expr, active_columns)?;
                let low = self.resolve_expr(low, active_columns)?;
                let high = self.resolve_expr(high, active_columns)?;
                if *negated {
                    Ok(Expression::Disjunctive {
                        terms: vec![
                            Expression::Comparative {
                                variant: ComparativeVariant::Lt,
                                left: Box::new(column.clone()),
                                right: Box::new(low),
                            },
                            Expression::Comparative {
                                variant: ComparativeVariant::Gt,
                                left: Box::new(column),
                                right: Box::new(high),
                            },
                        ],
                    })
                } else {
                    Ok(Expression::Conjunctive {
                        terms: vec![
                            Expression::Comparative {
                                variant: ComparativeVariant::Ge,
                                left: Box::new(column.clone()),
                                right: Box::new(low),
                            },
                            Expression::Comparative {
                                variant: ComparativeVariant::Lt,
                                left: Box::new(column),
                                right: Box::new(high),
                            },
                        ],
                    })
                }
            },

            Expr::BinaryOp { left: left_expr, op, right: right_expr} => {
                let left = self.resolve_expr(left_expr, active_columns)?;
                let right = self.resolve_expr(right_expr, active_columns)?;

                let comparative = |variant, left, right| Ok(Expression::Comparative {
                    variant,
                    left: Box::new(left),
                    right: Box::new(right),
                });

                match op {
                    BinaryOperator::Eq => comparative(ComparativeVariant::Eq, left, right),
                    BinaryOperator::Lt => comparative(ComparativeVariant::Lt, left, right),
                    BinaryOperator::LtEq => comparative(ComparativeVariant::Le, left, right),
                    BinaryOperator::Gt => comparative(ComparativeVariant::Gt, left, right),
                    BinaryOperator::GtEq => comparative(ComparativeVariant::Ge, left, right),
                    BinaryOperator::And => Ok(Expression::Conjunctive {
                        terms: vec![left, right],
                    }),
                    BinaryOperator::Or => Ok(Expression::Disjunctive {
                        terms: vec![left, right],
                    }),
                    _ => Err(format!("Unsupported binary operator {}", op))
                }
            },

            Expr::Identifier(ident) => {
                active_columns.iter()
                    .position(|column| column.get_name() == ident)
                    .map(|column| Expression::ColumnReference(column))
                    .ok_or(format!("Identifier {} not found", expr))
            },

            Expr::CompoundIdentifier(idents) => {
                if idents.len() == 2 {
                    active_columns.iter()
                        .position(|column|
                            column.get_table() == idents[0] && column.get_name() == idents[1]
                        )
                        .map(|column| Expression::ColumnReference(column))
                        .ok_or(format!("Identifier {} not found", expr))
                } else {
                    Err(format!("Invalid compound identifier {}", expr))
                }
            },

            Expr::Value(value) => {
                match value {
                    Value::Boolean(v) => {
                        let bool_type = Bool;
                        let buf = bool_type.new_buf(*v);
                        Ok(Expression::Literal {
                            type_variant: TypeVariant::Bool(bool_type),
                            buf,
                        })
                    },
                    Value::Long(v) => {
                        let v = i64::try_from(*v).map_err(|e| e.to_string())?;
                        let int64_type = Int64;
                        let buf = int64_type.new_buf(v);
                        Ok(Expression::Literal {
                            type_variant: TypeVariant::Int64(int64_type),
                            buf,
                        })
                    },
                    Value::SingleQuotedString(v) => {
                        let char_type = Char::new(v.len());
                        let buf = char_type.new_buf(v);
                        Ok(Expression::Literal {
                            type_variant: TypeVariant::Char(char_type),
                            buf,
                        })
                    },
                    _ => Err(format!("Unsupported literal type {}", expr)),
                }
            },

            _ => Err(format!("Unsupported expression type {}", expr)),
        }
    }

    fn resolve_literal(&self, expr: &Expr) -> Result<Vec<u8>, String> {
        self.resolve_expr(expr, &[]).and_then(|value|
            if let Expression::Literal { type_variant: _, buf} = value {
                Ok(buf)
            } else {
                Err(format!("Expected literal, found {}", expr))
            }
        )
    }
}

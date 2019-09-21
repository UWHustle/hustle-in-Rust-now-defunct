use std::collections::HashMap;
use std::sync::{Arc, mpsc, Mutex};
use std::sync::mpsc::Sender;

use hustle_catalog::{Catalog, Column, Table};
use hustle_common::plan::{Expression, Plan, Query, QueryOperator, Statement};
use hustle_storage::{LogManager, StorageManager};
use hustle_storage::block::{BlockReference, RowMask};

use crate::operator::{BeginTransaction, Cartesian, Collect, CommitTransaction, CreateTable, Delete, DropTable, Insert, Operator, Project, Select, TableReference, Update};
use crate::router::BlockPoolDestinationRouter;

pub type FinalizeRowIds = Arc<Mutex<HashMap<u64, HashMap<u64, Vec<u64>>>>>;

/// Hustle's execution engine. The `ExecutionEngine` is responsible for executing the plans
/// produced by the resolver/optimizer.
pub struct ExecutionEngine {
    catalog: Arc<Catalog>,
    storage_manager: StorageManager,
    log_manager: LogManager,
    finalize_row_ids: FinalizeRowIds,
}

impl ExecutionEngine {
    /// Returns a new `ExecutionEngine` with a reference to the `catalog` and the default storage
    /// manager configuration.
    pub fn new(catalog: Arc<Catalog>) -> Self {
        ExecutionEngine {
            catalog,
            storage_manager: StorageManager::default(),
            log_manager: LogManager::default(),
            finalize_row_ids: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Executes the specified `plan` and optionally returns an output `Table` if the plan is a
    /// query.
    pub fn execute_statement(&self, statement: Statement) -> Result<Option<Table>, String> {
        let operator = self.compile_statement(statement);
        let result = operator.downcast_ref::<Collect>().map(|collect| collect.get_result());
        operator.execute(&self.storage_manager, &self.log_manager, &self.catalog);
        let table = result.map(|r| r.into_table());
        Ok(table)
    }

    /// Iterates through the rows of the specified `table`, calling the function `f` on each row.
    /// This is used to return the rows of an output table to a client.
    pub fn get_rows(&self, table: Table, f: impl Fn(Vec<Vec<u8>>)) {
        // Send each row of the result.
        for block_id in table.block_ids {
            let block = self.storage_manager.get_block(block_id).unwrap();
            for row in block.rows() {
                let row = row.map(|buf| buf.to_vec()).collect();
                f(row);
            }
        }
    }

    fn compile_statement(&self, statement: Statement) -> Box<dyn Operator> {
        match statement.plan {
            Plan::BeginTransaction => Box::new(BeginTransaction::new(statement.transaction_id)),
            Plan::CommitTransaction => Box::new(CommitTransaction::new(
                statement.transaction_id,
                self.finalize_row_ids.clone()
            )),
            Plan::CreateTable(table) => Box::new(CreateTable::new(table)),
            Plan::DropTable(table) => Box::new(DropTable::new(table)),
            Plan::Insert { into_table, bufs } => {
                let table_name = into_table.name.clone();
                let router = BlockPoolDestinationRouter::with_block_ids(
                    into_table.block_ids,
                    into_table.columns,
                );
                Box::new(Insert::new(
                    table_name,
                    bufs,
                    router,
                    statement.transaction_id,
                    self.finalize_row_ids.clone(),
                ))
            },
            Plan::Update { table, assignments, filter } => {
                let filter = filter.map(|f| Self::compile_filter(*f, &table.columns));
                Box::new(Update::new(
                    assignments,
                    filter,
                    table.block_ids,
                    statement.transaction_id,
                ))
            },
            Plan::Delete { from_table, filter } => {
                let filter = filter.map(|f| Self::compile_filter(*f, &from_table.columns));
                Box::new(Delete::new(
                    filter,
                    from_table.block_ids,
                    statement.transaction_id,
                    self.finalize_row_ids.clone(),
                ))
            },
            Plan::Query(query) => {
                let cols = query.output.clone();
                let (block_tx, block_rx) = mpsc::channel();
                let mut operators = Vec::new();
                Self::compile_query(query, block_tx, &mut operators);
                Box::new(Collect::new(operators, cols, block_rx))
            },
        }
    }

    fn compile_query(query: Query, block_tx: Sender<u64>, operators: &mut Vec<Box<dyn Operator>>) {
        let router = BlockPoolDestinationRouter::new(query.output);
        match query.operator {
            QueryOperator::TableReference(table) => {
                operators.push(Box::new(TableReference::new(table, block_tx)));
            },
            QueryOperator::Project { input, cols } => {
                let (child_block_tx, block_rx) = mpsc::channel();
                Self::compile_query(*input, child_block_tx, operators);

                let project = Project::new(cols, router, block_rx, block_tx);
                operators.push(Box::new(project));
            },
            QueryOperator::Select { input, filter } => {
                let filter = Self::compile_filter(*filter, &input.output);
                let (child_block_tx, block_rx) = mpsc::channel();
                Self::compile_query(*input, child_block_tx, operators);

                let select = Select::new(filter, router, block_rx, block_tx);
                operators.push(Box::new(select));
            },
            QueryOperator::Cartesian { inputs } => {
                let block_rxs = inputs.into_iter()
                    .map(|input| {
                        let (child_block_tx, block_rx) = mpsc::channel();
                        Self::compile_query(input, child_block_tx, operators);
                        block_rx
                    })
                    .collect::<Vec<_>>();

                let cartesian = Cartesian::new(router, block_rxs, block_tx);
                operators.push(Box::new(cartesian));
            },
        }
    }

    fn compile_filter(
        filter: Expression,
        columns: &[Column],
    ) -> Box<dyn Fn(&BlockReference) -> RowMask> {
        match filter {
            Expression::Comparative { variant: comparative_variant, left, right } => {
                let l_col_i = match *left {
                    Expression::ColumnReference(column) => column,
                    _ => panic!("Only column references are allowed on left side of comparisons"),
                };
                let l_type_variant = columns[l_col_i].get_type_variant().clone();

                match *right {
                    Expression::Literal { buf: r_buf, type_variant: r_type_variant } => {
                        Box::new(move |block|
                            block.filter_col(l_col_i, |l_buf|
                                hustle_types::compare(
                                    comparative_variant,
                                    &l_type_variant,
                                    &r_type_variant,
                                    l_buf,
                                    &r_buf,
                                ).unwrap()
                            )
                        )
                    },
                    Expression::ColumnReference(r_col_i) => {
                        let r_type_variant = columns[r_col_i].get_type_variant().clone();
                        Box::new(move |block|
                            block.filter_cols(l_col_i, r_col_i, |l_buf, r_buf|
                                hustle_types::compare(
                                    comparative_variant,
                                    &l_type_variant,
                                    &r_type_variant,
                                    l_buf,
                                    r_buf,
                                ).unwrap()
                            )
                        )
                    },
                    _ => panic!("")
                }
            },
            Expression::Conjunctive { terms } => {
                let compiled_terms = terms.into_iter()
                    .map(|term| Self::compile_filter(term, columns))
                    .collect::<Vec<_>>();

                Box::new(move |block| {
                    let mut compiled_terms_iter = compiled_terms.iter();
                    let mut mask = (compiled_terms_iter.next().unwrap())(block);
                    for compiled_term in compiled_terms_iter {
                        mask.intersect(&(compiled_term)(block));
                    }
                    mask
                })
            },
            Expression::Disjunctive { terms } => {
                let compiled_terms = terms.into_iter()
                    .map(|term| Self::compile_filter(term, columns))
                    .collect::<Vec<_>>();

                Box::new(move |block| {
                    let mut compiled_terms_iter = compiled_terms.iter();
                    let mut mask = (compiled_terms_iter.next().unwrap())(block);
                    for compiled_term in compiled_terms_iter {
                        mask.union(&(compiled_term)(block));
                    }
                    mask
                })
            }
            _ => panic!("Unsupported expression node type"),
        }
    }
}

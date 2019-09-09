use std::sync::{Arc, mpsc};
use std::sync::mpsc::{Receiver, Sender};

use hustle_catalog::{Catalog, Column, Table};
use hustle_common::message::Message;
use hustle_common::plan::{Expression, Plan, Query, QueryOperator};
use hustle_storage::block::{BlockReference, RowMask};
use hustle_storage::StorageManager;

use crate::operator::{Cartesian, Collect, CreateTable, Delete, DropTable, Insert, Operator, Project, Select, TableReference, Update};
use crate::router::BlockPoolDestinationRouter;

pub struct ExecutionEngine {
    storage_manager: StorageManager,
    catalog: Arc<Catalog>,
}

impl ExecutionEngine {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        ExecutionEngine {
            storage_manager: StorageManager::default(),
            catalog
        }
    }

    pub fn execute_plan(&self, plan: Plan) -> Result<Option<Table>, String> {
        let operator = Self::compile_plan(plan);
        operator.execute(&self.storage_manager, &self.catalog);
        let table = operator.downcast_ref::<Collect>().map(|collect| collect.get_table());
        Ok(table)
    }

    pub fn listen(
        &mut self,
        execution_rx: Receiver<Message>,
        transaction_tx: Sender<Message>,
        completed_tx: Sender<Message>,
    ) {
        for message in execution_rx {
            if let Message::ExecutePlan { plan, statement_id, connection_id } = message {
                match self.execute_plan(plan) {
                    Ok(relation) => {

                        // The execution may have produced a result relation. If so, we send the
                        // rows back to the user.
                        if let Some(relation) = relation {

                            // Send a message with the result schema.
                            completed_tx.send(Message::Schema {
                                schema: relation.columns.clone(),
                                connection_id
                            }).unwrap();

                            // Send each row of the result.
                            for &block_id in &relation.block_ids {
                                let block = self.storage_manager.get_block(block_id).unwrap();
                                for row in block.rows() {
                                    completed_tx.send(Message::ReturnRow {
                                        row: row.map(|value| value.to_vec()).collect(),
                                        connection_id,
                                    }).unwrap();
                                }
                            }
                        }

                        // Send a success message to indicate completion.
                        completed_tx.send(Message::Success { connection_id }).unwrap();
                    },

                    Err(reason) => {
                        completed_tx.send(Message::Failure {
                            reason,
                            connection_id,
                        }).unwrap();
                    },
                };

                // Notify the transaction manager that the plan execution has completed.
                transaction_tx.send(Message::CompletePlan {
                    statement_id,
                    connection_id,
                }).unwrap();
            }
        }
    }

    fn compile_plan(plan: Plan) -> Box<dyn Operator> {
        match plan {
            Plan::CreateTable(table) => Box::new(CreateTable::new(table)),
            Plan::DropTable(table) => Box::new(DropTable::new(table)),
            Plan::Insert { into_table, bufs } => {
                let router = BlockPoolDestinationRouter::with_block_ids(
                    into_table.block_ids,
                    into_table.columns,
                );
                Box::new(Insert::new(bufs, router))
            },
            Plan::Update { table, assignments, filter } => {
                let filter = filter.map(|f| Self::compile_filter(*f, &table.columns));
                Box::new(Update::new(assignments, filter, table.block_ids))
            },
            Plan::Delete { from_table, filter } => {
                let filter = filter.map(|f| Self::compile_filter(*f, &from_table.columns));
                Box::new(Delete::new(filter, from_table.block_ids))
            },
            Plan::Query(query) => {
                let cols = query.output.clone();
                let (block_tx, block_rx) = mpsc::channel();
                let mut operators = Vec::new();
                Self::compile_query(query, block_tx, &mut operators);
                Box::new(Collect::new(operators, cols, block_rx))
            },
            _ => panic!("Unsupported plan: {:?}", plan),
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
                        mask.and(&(compiled_term)(block));
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
                        mask.or(&(compiled_term)(block));
                    }
                    mask
                })
            }
            _ => panic!("Unsupported expression node type"),
        }
    }
}

use std::sync::{Arc, mpsc};
use std::sync::mpsc::{Receiver, Sender};

use hustle_catalog::{Catalog, Table};
use hustle_common::message::Message;
use hustle_common::plan::{Expression, Plan, Query, QueryOperator};
use hustle_storage::block::{BlockReference, RowMask};
use hustle_storage::StorageManager;

use crate::operator::{Collect, CreateTable, DropTable, Insert, Operator, Select, TableReference, Update};
use crate::router::BlockPoolDestinationRouter;

pub struct ExecutionEngine {
    storage_manager: StorageManager,
    catalog: Arc<Catalog>,
}

impl ExecutionEngine {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        ExecutionEngine {
            storage_manager: StorageManager::new(),
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
                                let cols = (0..relation.columns.len()).collect::<Vec<usize>>();
                                for row in block.project(&cols) {
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
            Plan::CreateTable { table } => Box::new(CreateTable::new(table)),
            Plan::DropTable { table } => Box::new(DropTable::new(table)),
            Plan::Insert { into_table, bufs } => {
                let router = BlockPoolDestinationRouter::with_block_ids(
                    into_table.block_ids,
                    into_table.columns,
                );
                Box::new(Insert::new(bufs, router))
            },
            Plan::Update { table, columns, bufs, filter } => {
                let filter = filter.map(|f| Self::compile_filter(*f));
                Box::new(Update::new(columns, bufs, filter, table.block_ids))
            }
            Plan::Query { query } => {
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
        match query.operator {
            QueryOperator::TableReference { table } => {
                operators.push(Box::new(TableReference::new(table, block_tx)));
            },
            QueryOperator::Project { input, cols } => {
                let (child_block_tx, block_rx) = mpsc::channel();
                Self::compile_query(*input, child_block_tx, operators);

                let router = BlockPoolDestinationRouter::new(query.output);
                let project = Select::new(cols, None, router, block_rx, block_tx);
                operators.push(Box::new(project));
            },
            QueryOperator::Select { input, filter } => {
                let (child_block_tx, block_rx) = mpsc::channel();
                Self::compile_query(*input, child_block_tx, operators);

                let cols = (0..query.output.len()).collect::<Vec<usize>>();
                let filter = Self::compile_filter(*filter);
                let router = BlockPoolDestinationRouter::new(query.output);
                let select = Select::new(cols, Some(filter), router, block_rx, block_tx);
                operators.push(Box::new(select));
            },
            _ => panic!("Unsupported query: {:?}", query),
        }
    }

    fn compile_filter(_filter: Expression) -> Box<dyn Fn(&BlockReference) -> RowMask> {
        unimplemented!()
    }
}

use std::sync::{Arc, mpsc};
use std::sync::mpsc::{Receiver, Sender};

use hustle_catalog::{Catalog, Table};
use hustle_common::message::Message;
use hustle_common::plan::{Expression, Plan, Query, QueryOperator};
use hustle_storage::StorageManager;

use crate::operator::{Collect, CreateTable, DropTable, Operator, Project, Select, TableReference, Insert};
use crate::router::BlockPoolDestinationRouter;
use crate::predicate::Predicate;

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
        let operator_tree = Self::parse_plan(plan);
        operator_tree.execute(&self.storage_manager, &self.catalog);
        Ok(None)
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

    fn parse_plan(plan: Plan) -> Box<dyn Operator> {
        match plan {
            Plan::CreateTable { table } => Box::new(CreateTable::new(table)),
            Plan::DropTable { table } => Box::new(DropTable::new(table)),
//            Plan::Insert { into_table, values } => {
//                let schema = into_table.columns.iter().map(|c| c.column_type.size).collect();
//                let router = BlockPoolDestinationRouter::new(schema);
//                Box::new(Insert::new())
//            },
            Plan::Query { query } => {
                let (block_tx, block_rx) = mpsc::channel();
                let mut operators = Vec::new();
                Self::parse_query(query, block_tx, &mut operators);
                Box::new(Collect::new(block_rx, operators))
            },
            _ => panic!("Unsupported plan: {:?}", plan),
        }
    }

    fn parse_query(query: Query, block_tx: Sender<u64>, operators: &mut Vec<Box<dyn Operator>>) {
        match query.operator {
            QueryOperator::TableReference { table } => {
                operators.push(Box::new(TableReference::new(table, block_tx)));
            },
            QueryOperator::Project { input, cols } => {
                let (child_block_tx, block_rx) = mpsc::channel();
                Self::parse_query(*input, child_block_tx, operators);

                let schema = query.output_cols.iter().map(|c| c.data_type.size()).collect();
                let router = BlockPoolDestinationRouter::new(schema);
                let project = Project::new(block_rx, block_tx, router, cols);
                operators.push(Box::new(project));
            },
            QueryOperator::Select { input, filter } => {
                let (child_block_tx, block_rx) = mpsc::channel();
                Self::parse_query(*input, child_block_tx, operators);

                let schema = query.output_cols.iter().map(|c| c.data_type.size()).collect();
                let router = BlockPoolDestinationRouter::new(schema);
                let filter = Self::parse_filter(*filter);
                let select = Select::new(block_rx, block_tx, router, filter);
                operators.push(Box::new(select));
            },
            _ => panic!(""),
        }
    }

    fn parse_filter(filter: Expression) -> Box<dyn Predicate> {
        unimplemented!()
    }
}

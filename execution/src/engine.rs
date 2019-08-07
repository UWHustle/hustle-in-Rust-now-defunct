use std::sync::mpsc::{Receiver, Sender};
use std::sync::mpsc;

use hustle_catalog::Table;
use hustle_common::logical_plan::{Expression, Plan, Query};
use hustle_common::message::Message;
use hustle_storage::StorageManager;

use crate::operator::{CreateTable, DropTable, Operator};

pub struct ExecutionEngine {
    storage_manager: StorageManager,
}

impl ExecutionEngine {
    pub fn new() -> Self {
        ExecutionEngine {
            storage_manager: StorageManager::new(),
        }
    }

    pub fn execute_plan(&self, plan: Plan) -> Result<Option<Table>, String> {
        let operator_tree = Self::parse_plan(plan);
        operator_tree.execute(&self.storage_manager);
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
                                block.rows(|r| {
                                    let row = r.iter().map(|col| col.to_vec()).collect();
                                    completed_tx.send(Message::ReturnRow {
                                        row,
                                        connection_id,
                                    }).unwrap();
                                })
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
            Plan::Query { query } => Self::parse_query(query),
            Plan::CreateTable { table } => Box::new(CreateTable::new(table)),
            Plan::DropTable { table } => Box::new(DropTable::new(table)),
            _ => panic!("Unsupported plan: {:?}", plan),
        }
    }

    fn parse_query(query: Query) -> Box<dyn Operator> {
        unimplemented!()
    }

    fn parse_filter(filter: Expression) -> impl Fn(&[&[u8]]) -> bool {
        |_| false
    }
}

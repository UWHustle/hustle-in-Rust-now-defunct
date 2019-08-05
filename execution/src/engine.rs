use hustle_storage::StorageManager;
use std::sync::mpsc::{Receiver, Sender};
use hustle_common::{Message, Plan};
use hustle_catalog::Table;
use crate::operator::Operator;

pub struct ExecutionEngine {
    storage_manager: StorageManager
}

impl ExecutionEngine {
    pub fn new() -> Self {
        ExecutionEngine {
            storage_manager: StorageManager::new(),
        }
    }

    pub fn execute_plan(&self, plan: &Plan) -> Result<Option<Table>, String> {
        let operator_tree = self.generate_operator_tree();
        operator_tree.execute(&self.storage_manager)
    }

    pub fn listen(
        &mut self,
        execution_rx: Receiver<Message>,
        transaction_tx: Sender<Message>,
        completed_tx: Sender<Message>,
    ) {
        for message in execution_rx {
            if let Message::ExecutePlan { plan, statement_id, connection_id } = message {
                match self.execute_plan(&plan) {
                    Ok(relation) => {

                        // The execution may have produced a result relation. If so, we send the
                        // rows back to the user.
                        if let Some(relation) = relation {

                            // Send a message with the result schema.
                            completed_tx.send(Message::Schema {
                                schema: relation.columns.clone(),
                                connection_id
                            });

                            // Send each row of the result.
                            for &block_id in &relation.block_ids {
                                let block = self.storage_manager.get(block_id).unwrap();
                                block.rows(|r| {
                                    let row = r.iter().map(|col| col.to_owned()).collect();
                                    completed_tx.send(Message::ReturnRow {
                                        row,
                                        connection_id,
                                    });
                                })
                            }
                        }

                        // Send a success message to indicate completion.
                        completed_tx.send(Message::Success { connection_id });
                    },

                    Err(reason) => completed_tx.send(Message::Failure {
                        reason,
                        connection_id,
                    }),
                };

                // Notify the transaction manager that the plan execution has completed.
                transaction_tx.send(Message::CompletePlan {
                    statement_id,
                    connection_id,
                });
            }
        }
    }

    fn generate_operator_tree(&self) -> impl Operator {
        unimplemented!()
    }
}

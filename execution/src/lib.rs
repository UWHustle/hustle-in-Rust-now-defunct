pub mod logical_entities;
pub mod physical_operators;
pub mod physical_plan;
pub mod relational_api;
pub mod test_helpers;

use storage::StorageManager;
use logical_entities::relation::Relation;
use physical_plan::parser::parse;
use std::sync::mpsc::{Receiver, Sender};
use message::{Message, Plan};
use types::data_type::DataType;

extern crate storage;
extern crate message;
extern crate types;
extern crate core;

pub struct ExecutionEngine {
    pub storage_manager: StorageManager
}

impl ExecutionEngine {
    pub fn new() -> Self {
        ExecutionEngine {
            storage_manager: StorageManager::new()
        }
    }

    pub fn execute_plan(&self, plan: Plan) -> Result<Option<Relation>, String> {
        let node = parse(&plan)?;
        node.execute(&self.storage_manager);
        Ok(node.get_output_relation())
    }

    pub fn listen(
        &mut self,
        execution_rx: Receiver<Vec<u8>>,
        completed_tx: Sender<Vec<u8>>,
    ) {
        loop {
            let buf = execution_rx.recv().unwrap();
            let request = Message::deserialize(&buf).unwrap();
            let response = match request {
                Message::ExecutePlan { plan, connection_id } => {
                    match self.execute_plan(plan) {
                        Ok(relation) => {
                            if let Some(relation) = relation {
                                let schema = relation.get_schema();
                                let message_schema: Vec<(String, DataType)> = relation.get_schema()
                                    .get_columns().iter()
                                    .map(|c| (c.get_name().to_owned(), c.data_type()))
                                    .collect();

                                let response = Message::Schema {
                                    schema: message_schema,
                                    connection_id
                                };
                                completed_tx.send(response.serialize().unwrap()).unwrap();

                                let physical_relation = self.storage_manager
                                    .relational_engine()
                                    .get(relation.get_name())
                                    .unwrap();

                                for block in physical_relation.blocks() {
                                    for row_i in 0..block.get_n_rows() {
                                        let mut row = vec![];
                                        for col_i in 0..schema.get_columns().len() {
                                            let data = block.get_row_col(row_i, col_i).unwrap();
                                            row.push(data.to_owned());
                                        }
                                        completed_tx.send(Message::ReturnRow {
                                            row,
                                            connection_id
                                        }.serialize().unwrap()).unwrap();
                                    }
                                }
                            }
                            Message::Success { connection_id }
                        },

                        Err(reason) => {
                            Message::Failure { reason, connection_id }
                        }
                    }

                },
                _ => request
            };
            completed_tx.send(response.serialize().unwrap()).unwrap();
        }
    }
}

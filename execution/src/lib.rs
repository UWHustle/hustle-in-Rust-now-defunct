pub mod logical_entities;
pub mod physical_operators;
pub mod physical_plan;
pub mod relational_api;
pub mod test_helpers;

use hustle_storage::StorageManager;
use logical_entities::relation::Relation;
use physical_plan::parser::parse;
use std::sync::mpsc::{Receiver, Sender};
use hustle_common::{Message, Plan};
use hustle_types::data_type::DataType;

extern crate hustle_storage;
extern crate hustle_common;
extern crate hustle_types;
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

    pub fn execute_plan(&self, plan: &Plan) -> Result<Option<Relation>, String> {
        let node = parse(&plan)?;
        node.execute(&self.storage_manager);
        Ok(node.get_output_relation())
    }

    pub fn listen(
        &mut self,
        execution_rx: Receiver<Vec<u8>>,
        transaction_tx: Sender<Vec<u8>>,
        completed_tx: Sender<Vec<u8>>,
    ) {
        loop {
            let buf = execution_rx.recv().unwrap();
            let request = Message::deserialize(&buf).unwrap();
            match request {
                Message::ExecutePlan { plan, statement_id, connection_id } => {
                    match self.execute_plan(&plan) {
                        Ok(relation) => {
                            if let Some(relation) = relation {
                                let schema = relation.get_schema();
                                let message_schema: Vec<(String, DataType)> = relation.get_schema()
                                    .get_columns().iter()
                                    .map(|c| (c.get_name().to_owned(), c.data_type()))
                                    .collect();

                                let response = Message::Schema {
                                    schema: message_schema,
                                    connection_id,
                                };
                                completed_tx.send(response.serialize().unwrap()).unwrap();
                                self.return_rows(&relation, connection_id, &completed_tx)
                            };
                            completed_tx.send(Message::Success {
                                connection_id,
                            }.serialize().unwrap()).unwrap();
                        }

                        Err(reason) => completed_tx.send(Message::Failure {
                            reason,
                            connection_id,
                        }.serialize().unwrap()).unwrap()
                    };

                    transaction_tx.send(Message::CompletePlan {
                        statement_id,
                        connection_id,
                    }.serialize().unwrap()).unwrap();
                },
                _ => completed_tx.send(buf).unwrap()
            };
        }
    }

    fn return_rows(&self, relation: &Relation, connection_id: u64, completed_tx: &Sender<Vec<u8>>) {
        let physical_relation = self.storage_manager
            .relational_engine()
            .get(relation.get_name())
            .unwrap();

        for block in physical_relation.blocks() {
            block.rows(|row_raw| {
                let row: Vec<Vec<u8>> = row_raw.iter().map(|col| col.to_vec()).collect();
                completed_tx.send(Message::ReturnRow {
                    row,
                    connection_id,
                }.serialize().unwrap()).unwrap();
            });
        };
    }
}



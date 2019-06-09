pub mod logical_entities;
pub mod physical_operators;
pub mod physical_plan;
pub mod relational_api;
pub mod test_helpers;

use physical_plan::parser::parse;
use storage::StorageManager;
use logical_entities::relation::Relation;
use std::sync::mpsc::{Receiver, Sender};
use message::Message;
use types::data_type::DataType;

extern crate storage;
extern crate message;
extern crate types;

pub struct ExecutionEngine {
    storage_manager: StorageManager
}

impl ExecutionEngine {
    pub fn new() -> Self {
        ExecutionEngine {
            storage_manager: StorageManager::new()
        }
    }

    pub fn execute_plan(&self, plan_string: &str) -> Option<Relation> {
        let node = parse(plan_string);
        node.execute(&self.storage_manager);
        node.get_output_relation()
    }

    pub fn listen(&mut self, input_rx: Receiver<Vec<u8>>, output_tx: Sender<Vec<u8>>) {
        loop {
            let buf = input_rx.recv().unwrap();
            let request = Message::deserialize(&buf).unwrap();
            match request {
                Message::ExecutePlan { plan, connection_id } => {
                    if let Some(relation) = self.execute_plan(&plan) {
                        let schema = relation.get_schema();
                        let message_schema: Vec<(String, DataType)> = relation.get_schema()
                            .get_columns().iter()
                            .map(|c| (c.get_name().to_owned(), c.data_type()))
                            .collect();

                        let response = Message::Schema { schema: message_schema, connection_id };
                        output_tx.send(response.serialize().unwrap()).unwrap();

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
                                let response = Message::ReturnRow { row, connection_id };
                                output_tx.send(response.serialize().unwrap()).unwrap();
                            }
                        }
                    }

                    let response = Message::Success { connection_id };
                    output_tx.send(response.serialize().unwrap()).unwrap();
                },
                _ => panic!("Invalid message type sent to execution engine")
            }
        }
    }

    pub fn get_storage_manager(&self) -> &StorageManager {
        &self.storage_manager
    }
}

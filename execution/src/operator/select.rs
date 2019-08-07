use std::sync::mpsc::{Receiver, Sender};

use hustle_storage::StorageManager;

use crate::operator::Operator;
use crate::router::BlockPoolDestinationRouter;

pub struct Select<F> {
    input_block_ids: Receiver<u64>,
    output_block_ids: Sender<u64>,
    destination_router: BlockPoolDestinationRouter,
    output_schema: Vec<usize>,
    filter: F,
}

impl<F: Fn(&[&[u8]]) -> bool> Select<F> {
    pub fn new(
        input_block_ids: Receiver<u64>,
        output_block_ids: Sender<u64>,
        destination_router: BlockPoolDestinationRouter,
        input_schema: Vec<usize>,
        filter: F,
    ) -> Self {
        Select {
            input_block_ids,
            output_block_ids,
            destination_router,
            output_schema: input_schema,
            filter,
        }
    }
}

impl<F: Fn(&[&[u8]]) -> bool> Operator for Select<F> {
    fn execute(&self, storage_manager: &StorageManager) {
        let output_block = self.destination_router.get_block(
            storage_manager,
            &self.output_schema,
        );

        for block_id in &self.input_block_ids {
            let input_block = storage_manager.get_block(block_id).unwrap();

            input_block.rows(|row| {
                if output_block.is_full() {
                    self.output_block_ids.send(output_block.id).unwrap();
                    let output_block = self.destination_router.get_block(
                        storage_manager,
                        &self.output_schema,
                    );
                }

                if (self.filter)(row) {
                    output_block.insert(row);
                }
            })
        }
    }
}

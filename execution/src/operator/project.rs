use std::sync::mpsc::{Receiver, Sender};

use hustle_storage::StorageManager;

use crate::operator::Operator;
use crate::router::BlockPoolDestinationRouter;

pub struct Project {
    input_block_ids: Receiver<u64>,
    output_block_ids: Sender<u64>,
    destination_router: BlockPoolDestinationRouter,
    output_schema: Vec<usize>,
    cols: Vec<usize>,
}

impl Project {
    pub fn new(
        input_block_ids: Receiver<u64>,
        output_block_ids: Sender<u64>,
        destination_router: BlockPoolDestinationRouter,
        input_schema: Vec<usize>,
        cols: Vec<usize>,
    ) -> Self {
        let output_schema = cols.iter()
            .map(|&col| input_schema[col])
            .collect();
        Project {
            input_block_ids,
            output_block_ids,
            destination_router,
            output_schema,
            cols,
        }
    }
}

impl Operator for Project {
    fn execute(&self, storage_manager: &StorageManager) {
        let mut output_block = self.destination_router.get_block(
            storage_manager,
            &self.output_schema
        );

        for block_id in &self.input_block_ids {
            let input_block = storage_manager.get_block(block_id).unwrap();
            let mut rows = input_block.project(&self.cols).peekable();
            while rows.peek().is_some() {
                output_block.insert(&mut rows);
                if output_block.is_full() {
                    self.output_block_ids.send(output_block.id).unwrap();
                    output_block = self.destination_router.get_block(
                        storage_manager,
                        &self.output_schema,
                    );
                }
            }
        }
    }
}

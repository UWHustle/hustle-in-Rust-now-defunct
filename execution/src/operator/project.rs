use std::sync::mpsc::{Receiver, Sender};

use hustle_storage::StorageManager;

use crate::operator::Operator;
use crate::router::BlockPoolDestinationRouter;
use hustle_catalog::{Column, Catalog};

pub struct Project {
    block_rx: Receiver<u64>,
    block_tx: Sender<u64>,
    output_cols: Vec<Column>,
    router: BlockPoolDestinationRouter,
    cols: Vec<usize>,
}

impl Project {
    pub fn new(
        block_rx: Receiver<u64>,
        block_tx: Sender<u64>,
        output_cols: Vec<Column>,
        router: BlockPoolDestinationRouter,
        cols: Vec<usize>,
    ) -> Self {
        Project {
            block_rx,
            block_tx,
            output_cols,
            router,
            cols,
        }
    }
}

impl Operator for Project {
    fn execute(&self, storage_manager: &StorageManager, catalog: &Catalog) {
        let output_schema = self.output_cols.iter()
            .map(|col| col.column_type.size)
            .collect::<Vec<usize>>();
        let mut output_block = self.router.get_block(storage_manager, &output_schema);

        for block_id in &self.block_rx {
            let input_block = storage_manager.get_block(block_id).unwrap();
            let mut rows = input_block.project(&self.cols).peekable();
            while rows.peek().is_some() {
                output_block.insert(&mut rows);
                if output_block.is_full() {
                    self.block_tx.send(output_block.id).unwrap();
                    output_block = self.router.get_block(
                        storage_manager,
                        &output_schema,
                    );
                }
            }
        }
    }
}

use std::sync::mpsc::{Receiver, Sender};

use hustle_catalog::Catalog;
use hustle_storage::StorageManager;

use crate::operator::{Operator, util};
use crate::router::BlockPoolDestinationRouter;

pub struct Project {
    cols: Vec<usize>,
    router: BlockPoolDestinationRouter,
    block_rx: Receiver<u64>,
    block_tx: Sender<u64>,
}

impl Project {
    pub fn new(
        cols: Vec<usize>,
        router: BlockPoolDestinationRouter,
        block_rx: Receiver<u64>,
        block_tx: Sender<u64>,
    ) -> Self {
        Project {
            cols,
            router,
            block_rx,
            block_tx,
        }
    }
}

impl Operator for Project {
    fn execute(&self, storage_manager: &StorageManager, _catalog: &Catalog) {
        let mut output_block = self.router.get_block(storage_manager);

        for input_block_id in &self.block_rx {
            let input_block = storage_manager.get_block(input_block_id).unwrap();
            let rows = input_block.project(&self.cols);
            util::send_rows(
                rows,
                &mut output_block,
                &self.block_tx,
                &self.router,
                storage_manager,
            );
        }

        self.block_tx.send(output_block.id).unwrap();
    }
}

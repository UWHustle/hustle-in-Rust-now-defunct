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
    fn execute(self: Box<Self>, storage_manager: &StorageManager, _catalog: &Catalog) {
        for input_block_id in &self.block_rx {
            let input_block = storage_manager.get_block(input_block_id).unwrap();
            let mut rows = input_block.project(&self.cols);
            util::send_rows(
                &mut rows,
                &self.block_tx,
                &self.router,
                storage_manager,
            );
        }

        for block_id in self.router.get_all_block_ids() {
            self.block_tx.send(block_id).unwrap()
        }
    }
}

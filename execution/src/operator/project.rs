use std::sync::mpsc::{Receiver, Sender};

use hustle_catalog::Catalog;
use hustle_storage::StorageManager;

use crate::operator::Operator;
use crate::router::BlockPoolDestinationRouter;

pub struct Project {
    block_rx: Receiver<u64>,
    block_tx: Sender<u64>,
    router: BlockPoolDestinationRouter,
    cols: Vec<usize>,
}

impl Project {
    pub fn new(
        block_rx: Receiver<u64>,
        block_tx: Sender<u64>,
        router: BlockPoolDestinationRouter,
        cols: Vec<usize>,
    ) -> Self {
        Project {
            block_rx,
            block_tx,
            router,
            cols,
        }
    }
}

impl Operator for Project {
    fn execute(&self, storage_manager: &StorageManager, _catalog: &Catalog) {
        unimplemented!()
    }
}

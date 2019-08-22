use std::sync::mpsc::{Receiver, Sender};

use bit_vec::BitVec;

use hustle_catalog::Catalog;
use hustle_storage::block::BlockReference;
use hustle_storage::StorageManager;

use crate::operator::Operator;
use crate::router::BlockPoolDestinationRouter;

pub struct Select {
    block_rx: Receiver<u64>,
    block_tx: Sender<u64>,
    router: BlockPoolDestinationRouter,
    filter: Box<dyn Fn(&BlockReference) -> BitVec>,
}

impl Select {
    pub fn new(
        block_rx: Receiver<u64>,
        block_tx: Sender<u64>,
        router: BlockPoolDestinationRouter,
        filter: Box<dyn Fn(&BlockReference) -> BitVec>,
    ) -> Self {
        Select {
            block_rx,
            block_tx,
            router,
            filter,
        }
    }
}

impl Operator for Select {
    fn execute(&self, storage_manager: &StorageManager, _catalog: &Catalog) {
        unimplemented!()
    }
}

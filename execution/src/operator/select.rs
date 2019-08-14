use std::sync::mpsc::{Receiver, Sender};

use hustle_catalog::Catalog;
use hustle_storage::StorageManager;

use crate::operator::Operator;
use crate::router::BlockPoolDestinationRouter;
use crate::predicate::Predicate;

pub struct Select {
    block_rx: Receiver<u64>,
    block_tx: Sender<u64>,
    router: BlockPoolDestinationRouter,
    filter: Box<dyn Predicate>,
}

impl Select {
    pub fn new(
        block_rx: Receiver<u64>,
        block_tx: Sender<u64>,
        router: BlockPoolDestinationRouter,
        filter: Box<dyn Predicate>,
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
        let mut output_block = self.router.get_block(storage_manager);
        for block_id in &self.block_rx {
            let input_block = storage_manager.get_block(block_id).unwrap();
            let mask = self.filter.evaluate(&input_block);
            let mut rows = input_block.rows_with_mask(&mask).peekable();
            while rows.peek().is_some() {
                output_block.extend(&mut rows);
                self.block_tx.send(output_block.id).unwrap();
                output_block = self.router.get_block(storage_manager);
            }
        }
    }
}

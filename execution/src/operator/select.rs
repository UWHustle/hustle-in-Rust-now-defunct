use std::sync::mpsc::{Receiver, Sender};

use hustle_catalog::Catalog;
use hustle_storage::block::{BlockReference, RowMask};
use hustle_storage::StorageManager;

use crate::operator::Operator;
use crate::router::BlockPoolDestinationRouter;

pub struct Select {
    cols: Vec<usize>,
    filter: Option<Box<dyn Fn(&BlockReference) -> RowMask>>,
    router: BlockPoolDestinationRouter,
    block_rx: Receiver<u64>,
    block_tx: Sender<u64>,
}

impl Select {
    pub fn new(
        cols: Vec<usize>,
        filter: Option<Box<dyn Fn(&BlockReference) -> RowMask>>,
        router: BlockPoolDestinationRouter,
        block_rx: Receiver<u64>,
        block_tx: Sender<u64>,
    ) -> Self {
        Select {
            cols,
            filter,
            router,
            block_rx,
            block_tx,
        }
    }

    fn send_rows<'a>(
        &self,
        rows: impl Iterator<Item = impl Iterator<Item = &'a [u8]>>,
        output_block: &mut BlockReference,
        storage_manager: &StorageManager,
    ) {
        let mut rows = rows.peekable();
        while rows.peek().is_some() {
            output_block.insert_rows(&mut rows);

            if output_block.is_full() {
                self.block_tx.send(output_block.id).unwrap();
                *output_block = self.router.get_block(storage_manager);
            }
        }
    }
}

impl Operator for Select {
    fn execute(&self, storage_manager: &StorageManager, _catalog: &Catalog) {
        let mut output_block = self.router.get_block(storage_manager);

        for input_block_id in &self.block_rx {
            let input_block = storage_manager.get_block(input_block_id).unwrap();

            if let Some(filter) = &self.filter {
                let mask = (filter)(&input_block);
                let rows = input_block.project_with_mask(&self.cols, &mask);
                self.send_rows(rows, &mut output_block, storage_manager);
            } else {
                let rows = input_block.project(&self.cols);
                self.send_rows(rows, &mut output_block, storage_manager);
            }
        }

        self.block_tx.send(output_block.id).unwrap();
    }
}

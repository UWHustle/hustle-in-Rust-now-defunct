use std::sync::mpsc::{Receiver, Sender};

use hustle_catalog::Catalog;
use hustle_storage::{LogManager, StorageManager};
use hustle_storage::block::{BlockReference, RowMask};

use crate::operator::{Operator, util};
use crate::router::BlockPoolDestinationRouter;

pub struct Select {
    filter: Box<dyn Fn(&BlockReference) -> RowMask>,
    router: BlockPoolDestinationRouter,
    block_rx: Receiver<u64>,
    block_tx: Sender<u64>,
}

impl Select {
    pub fn new(
        filter: Box<dyn Fn(&BlockReference) -> RowMask>,
        router: BlockPoolDestinationRouter,
        block_rx: Receiver<u64>,
        block_tx: Sender<u64>,
    ) -> Self {
        Select {
            filter,
            router,
            block_rx,
            block_tx,
        }
    }
}

impl Operator for Select {
    fn execute(
        self: Box<Self>,
        storage_manager: &StorageManager,
        _log_manager: &LogManager,
        _catalog: &Catalog
    ) {
        for input_block_id in &self.block_rx {
            let input_block = storage_manager.get_block(input_block_id).unwrap();
            let mask = (self.filter)(&input_block);
            let mut rows = input_block.rows_with_mask(&mask);
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

#[cfg(test)]
mod select_tests {
    use std::mem;
    use std::sync::mpsc;

    use hustle_execution_test_util as test_util;
    use hustle_types::Bool;

    use crate::router::BlockPoolDestinationRouter;

    use super::*;

    #[test]
    fn select() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let log_manager = LogManager::with_unique_log_directory();
        let catalog = Catalog::new();
        let table = test_util::example_table();
        let input_block = test_util::example_block(&storage_manager);
        let router = BlockPoolDestinationRouter::new(table.columns);

        let (input_block_tx, input_block_rx) = mpsc::channel();
        let (output_block_tx, output_block_rx) = mpsc::channel();

        input_block_tx.send(input_block.id).unwrap();
        mem::drop(input_block_tx);

        let filter = Box::new(|block: &BlockReference|
            block.filter_col(0, |buf| Bool.get(buf))
        );

        let select = Box::new(Select::new(filter, router, input_block_rx, output_block_tx));
        select.execute(&storage_manager, &log_manager, &catalog);

        let output_block = storage_manager.get_block(output_block_rx.recv().unwrap()).unwrap();

        assert_eq!(output_block.get_row_col(0, 0), input_block.get_row_col(1, 0));
        assert_eq!(output_block.get_row_col(0, 1), input_block.get_row_col(1, 1));
        assert_eq!(output_block.get_row_col(0, 2), input_block.get_row_col(1, 2));
        assert_eq!(output_block.get_row_col(0, 3), None);

        storage_manager.clear();
        log_manager.clear();
    }
}

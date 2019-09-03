use std::sync::mpsc::{Receiver, Sender};

use hustle_catalog::Catalog;
use hustle_storage::block::{BlockReference, RowMask};
use hustle_storage::StorageManager;

use crate::operator::{Operator, util};
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
}

impl Operator for Select {
    fn execute(&self, storage_manager: &StorageManager, _catalog: &Catalog) {
        let mut output_block = self.router.get_block(storage_manager);

        for input_block_id in &self.block_rx {
            let input_block = storage_manager.get_block(input_block_id).unwrap();

            if let Some(filter) = &self.filter {
                let mask = (filter)(&input_block);
                let rows = input_block.project_with_mask(&self.cols, &mask);
                util::send_rows(
                    rows,
                    &mut output_block,
                    &self.block_tx,
                    &self.router,
                    storage_manager,
                );
            } else {
                let rows = input_block.project(&self.cols);
                util::send_rows(
                    rows,
                    &mut output_block,
                    &self.block_tx,
                    &self.router,
                    storage_manager,
                );
            }
        }

        self.block_tx.send(output_block.id).unwrap();
    }
}

#[cfg(test)]
mod select_tests {
    use std::mem;
    use std::sync::mpsc;

    use hustle_catalog::{Catalog, Column, Table};
    use hustle_execution_test_util as test_util;
    use hustle_storage::StorageManager;
    use hustle_types::{Bool, Int64, TypeVariant};

    use crate::router::BlockPoolDestinationRouter;

    use super::*;

    #[test]
    fn project() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let catalog = Catalog::new();
        let project_table = Table::new(
            "project".to_owned(),
            vec![Column::new("col_int64".to_owned(), TypeVariant::Int64(Int64), false)],
            vec![],
        );
        let input_block = test_util::example_block(&storage_manager);
        let router = BlockPoolDestinationRouter::new(project_table.columns);

        let (input_block_tx, input_block_rx) = mpsc::channel();
        let (output_block_tx, output_block_rx) = mpsc::channel();

        input_block_tx.send(input_block.id).unwrap();
        mem::drop(input_block_tx);

        let project = Select::new(vec![1], None, router, input_block_rx, output_block_tx);
        project.execute(&storage_manager, &catalog);

        let output_block = storage_manager.get_block(output_block_rx.recv().unwrap()).unwrap();

        assert_eq!(output_block.get_row_col(0, 0), input_block.get_row_col(0, 1));
        assert_eq!(output_block.get_row_col(1, 0), input_block.get_row_col(1, 1));
        assert_eq!(output_block.get_row_col(2, 0), None);
        assert_eq!(output_block.get_row_col(0, 1), None);

        storage_manager.clear();
    }

    #[test]
    fn select() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let catalog = Catalog::new();
        let select_table = test_util::example_table();
        let input_block = test_util::example_block(&storage_manager);
        let router = BlockPoolDestinationRouter::new(select_table.columns);

        let (input_block_tx, input_block_rx) = mpsc::channel();
        let (output_block_tx, output_block_rx) = mpsc::channel();

        input_block_tx.send(input_block.id).unwrap();
        mem::drop(input_block_tx);

        let filter = Box::new(|block: &BlockReference|
            block.filter_col(0, |buf| Bool.get(buf))
        );

        let select = Select::new(vec![0, 1, 2], Some(filter), router, input_block_rx, output_block_tx);
        select.execute(&storage_manager, &catalog);

        let output_block = storage_manager.get_block(output_block_rx.recv().unwrap()).unwrap();

        assert_eq!(output_block.get_row_col(0, 0), input_block.get_row_col(1, 0));
        assert_eq!(output_block.get_row_col(0, 1), input_block.get_row_col(1, 1));
        assert_eq!(output_block.get_row_col(0, 2), input_block.get_row_col(1, 2));
        assert_eq!(output_block.get_row_col(0, 3), None);

        storage_manager.clear();
    }
}

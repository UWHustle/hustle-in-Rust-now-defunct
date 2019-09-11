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

#[cfg(test)]
mod project_tests {
    use std::mem;
    use std::sync::mpsc;

    use hustle_catalog::{Column, Table};
    use hustle_execution_test_util as test_util;
    use hustle_types::{Int64, TypeVariant};

    use super::*;

    #[test]
    fn project() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let catalog = Catalog::new();
        let project_table = Table::new(
            "project".to_owned(),
            vec![Column::new(
                "col_int64".to_owned(),
                "project".to_owned(),
                TypeVariant::Int64(Int64),
                false,
            )],
        );
        let input_block = test_util::example_block(&storage_manager);
        let router = BlockPoolDestinationRouter::new(project_table.columns);

        let (input_block_tx, input_block_rx) = mpsc::channel();
        let (output_block_tx, output_block_rx) = mpsc::channel();

        input_block_tx.send(input_block.id).unwrap();
        mem::drop(input_block_tx);

        let project = Box::new(Project::new(vec![1], router, input_block_rx, output_block_tx));
        project.execute(&storage_manager, &catalog);

        let output_block = storage_manager.get_block(output_block_rx.recv().unwrap()).unwrap();

        assert_eq!(output_block.get_row_col(0, 0), input_block.get_row_col(0, 1));
        assert_eq!(output_block.get_row_col(1, 0), input_block.get_row_col(1, 1));
        assert_eq!(output_block.get_row_col(2, 0), None);
        assert_eq!(output_block.get_row_col(0, 1), None);

        storage_manager.clear();
    }
}



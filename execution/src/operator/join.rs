use std::sync::mpsc::{Receiver, Sender};
use std::sync::mpsc;
use std::thread;

use itertools::Itertools;

use hustle_catalog::Catalog;
use hustle_storage::StorageManager;

use crate::operator::{Operator, util};
use crate::router::BlockPoolDestinationRouter;

pub struct Join {
    filter: Option<Box<dyn Fn(&[&[u8]]) -> bool>>,
    router: BlockPoolDestinationRouter,
    n_tables: usize,
    block_rx: Receiver<(usize, u64)>,
    block_tx: Sender<u64>,
}

impl Join {
    pub fn new(
        filter: Option<Box<dyn Fn(&[&[u8]]) -> bool>>,
        router: BlockPoolDestinationRouter,
        block_rxs: Vec<Receiver<u64>>,
        block_tx: Sender<u64>,
    ) -> Self {
        // Merge the vector of receivers into a single receiver.
        let n_tables = block_rxs.len();
        let (input_block_tx, block_rx) = mpsc::channel();
        for (i, block_rx) in block_rxs.into_iter().enumerate() {
            let input_block_tx = input_block_tx.clone();
            thread::spawn(move || {
                for block_id in block_rx {
                    input_block_tx.send((i, block_id)).unwrap();
                }
            });
        }

        Join {
            filter,
            router,
            n_tables,
            block_rx,
            block_tx,
        }
    }

    fn execute_with_block_ids(&self, block_ids: &[u64], storage_manager: &StorageManager) {
        let blocks = block_ids.iter()
            .map(|&block_id| storage_manager.get_block(block_id).unwrap())
            .collect::<Vec<_>>();

        let rows = blocks.iter()
            .map(|block| block.rows())
            .multi_cartesian_product()
            .map(|row|
                row.iter().flat_map(|r| r.clone()).collect::<Vec<_>>()
            );


        let mut output_block = self.router.get_block(storage_manager);

        if let Some(filter) = &self.filter {
            let rows = rows
                .filter(|row| (**filter)(row))
                .map(|row| row.into_iter());
            util::send_rows(rows, &mut output_block, &self.block_tx, &self.router, storage_manager);
        } else {
            let rows = rows
                .map(|row| row.into_iter());
            util::send_rows(rows, &mut output_block, &self.block_tx, &self.router, storage_manager);
        }

        self.block_tx.send(output_block.id).unwrap();
    }
}

impl Operator for Join {
    fn execute(&self, storage_manager: &StorageManager, _catalog: &Catalog) {
        let mut table_block_ids = (0..self.n_tables).map(|_| vec![]).collect::<Vec<Vec<u64>>>();
        for (received_table, received_block_id) in &self.block_rx {
            table_block_ids[received_table].push(received_block_id);

            let block_ids_product = table_block_ids.iter()
                .map(|table_block_ids| table_block_ids.iter().cloned())
                .multi_cartesian_product()
                .filter(|block_ids| block_ids[received_table] == received_block_id);

            for block_ids in block_ids_product {
                self.execute_with_block_ids(&block_ids, storage_manager);
            }
        }
    }
}

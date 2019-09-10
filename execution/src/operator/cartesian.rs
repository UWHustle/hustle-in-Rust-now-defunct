use std::sync::mpsc::{Receiver, Sender};
use std::sync::mpsc;
use std::thread;

use itertools::Itertools;

use hustle_catalog::Catalog;
use hustle_storage::StorageManager;

use crate::operator::{Operator, util};
use crate::router::BlockPoolDestinationRouter;

pub struct Cartesian {
    router: BlockPoolDestinationRouter,
    n_tables: usize,
    block_rx: Receiver<(usize, u64)>,
    block_tx: Sender<u64>,
}

impl Cartesian {
    pub fn new(
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

        Cartesian {
            router,
            n_tables,
            block_rx,
            block_tx,
        }
    }
}

impl Operator for Cartesian {
    fn execute(self: Box<Self>, storage_manager: &StorageManager, _catalog: &Catalog) {
        let mut table_block_ids = (0..self.n_tables).map(|_| vec![]).collect::<Vec<Vec<u64>>>();

        for (received_table, received_block_id) in &self.block_rx {
            table_block_ids[received_table].push(received_block_id);

            let block_ids_product = table_block_ids.iter()
                .map(|table_block_ids| table_block_ids.iter().cloned())
                .multi_cartesian_product()
                .filter(|block_ids| block_ids[received_table] == received_block_id);

            for block_ids in block_ids_product {
                let blocks = block_ids.iter()
                    .map(|&block_id| storage_manager.get_block(block_id).unwrap())
                    .collect::<Vec<_>>();

                let mut rows = blocks.iter()
                    .map(|block| block.rows())
                    .multi_cartesian_product()
                    .map(|row| row.into_iter().flat_map(|r| r.clone()));

                util::send_rows(&mut rows, &self.block_tx, &self.router, storage_manager)
            }
        }

        for block_id in self.router.get_all_block_ids() {
            self.block_tx.send(block_id).unwrap()
        }
    }
}

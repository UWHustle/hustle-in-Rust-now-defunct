use std::sync::{Arc, mpsc};
use std::sync::mpsc::{Receiver, Sender};
use std::thread;

use itertools::Itertools;

use hustle_catalog::Catalog;
use hustle_storage::{LogManager, StorageManager};

use crate::operator::{Operator, util};
use crate::router::BlockPoolDestinationRouter;
use crate::state::TransactionState;

pub struct Cartesian {
    router: BlockPoolDestinationRouter,
    n_tables: usize,
    block_rx: Receiver<(usize, u64)>,
    block_tx: Sender<u64>,
    transaction_state: Arc<TransactionState>,
}

impl Cartesian {
    pub fn new(
        router: BlockPoolDestinationRouter,
        block_rxs: Vec<Receiver<u64>>,
        block_tx: Sender<u64>,
        transaction_state: Arc<TransactionState>,
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
            transaction_state,
        }
    }
}

impl Operator for Cartesian {
    fn execute(
        self: Box<Self>,
        storage_manager: &StorageManager,
        _log_manager: &LogManager,
        _catalog: &Catalog,
    ) {
        println!("execute cartesian");

        let mut table_block_ids = (0..self.n_tables).map(|_| vec![]).collect::<Vec<Vec<u64>>>();

        for (received_table, received_block_id) in &self.block_rx {
            println!("{}", received_block_id);
            table_block_ids[received_table].push(received_block_id);

            // Get the cartesian product of all received blocks prior, only considering the block
            // that has just been received.
            let block_ids_product = table_block_ids.iter()
                .map(|table_block_ids| table_block_ids.iter().cloned())
                .multi_cartesian_product()
                .filter(|block_ids| block_ids[received_table] == received_block_id);

            for block_ids in block_ids_product {
                let blocks = block_ids.iter()
                    .map(|&block_id| storage_manager.get_block(block_id).unwrap())
                    .collect::<Vec<_>>();

                let include_tentatives = block_ids.iter()
                    .map(|&block_id| self.transaction_state.lock_inserted_for_block(block_id))
                    .collect::<Vec<_>>();

                println!("included_tentatives");

                // Get the cartesian product of the rows from the blocks.
                let mut rows = blocks.iter().zip(&include_tentatives)
                    .map(|(block, include_tentative)| block.rows(include_tentative))
                    .multi_cartesian_product()
                    .map(|row| row.into_iter().flat_map(|r| r.clone()));

                util::send_rows(rows, &self.block_tx, &self.router, storage_manager);
                println!("sent rows");
            }
        }

        println!("sending remaining");


        for block_id in self.router.get_all_block_ids() {
            self.block_tx.send(block_id).unwrap()
        }
    }
}

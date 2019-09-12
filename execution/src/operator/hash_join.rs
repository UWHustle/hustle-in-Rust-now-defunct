use std::sync::mpsc::{Receiver, Sender};

use hustle_catalog::Catalog;
use hustle_storage::StorageManager;

use crate::operator::{Operator, util};
use crate::router::BlockPoolDestinationRouter;

use std::collections::HashMap;

pub struct Hash_Join {
    router: BlockPoolDestinationRouter,
    block_rx_table_1: Receiver<(u64)>,
    block_rx_table_2: Receiver<(u64)>,
    block_tx: Sender<u64>,
    join_attribute_table_1: usize,
    join_attribute_table_2: usize,
}

impl Hash_Join {
    pub fn new(
        router: BlockPoolDestinationRouter,
        block_rx_table_1: Receiver<u64>,
        block_rx_table_2: Receiver<u64>,
        block_tx: Sender<u64>,
        join_attribute_table_1: usize,
        join_attribute_table_2: usize,
    ) -> Self {
        Hash_Join {
            router,
            block_rx_table_1,
            block_rx_table_2,
            block_tx,
            join_attribute_table_1,
            join_attribute_table_2,
        }
    }
}

impl Operator for Hash_Join {
    fn execute(self: Box<Self>, storage_manager: &StorageManager, _catalog: &Catalog) {
        let mut hash_table = HashMap::new();

        // BUILD PHASE
        for input_block_id in &self.block_rx_table_1 {
            let input_block = storage_manager.get_block(input_block_id).unwrap();

            for (row_id, key) in input_block.get_col(self.join_attribute_table_1).unwrap() {
                let rows_with_col_value = hash_table.entry(key.to_vec()).or_insert(Vec::new());
                rows_with_col_value.push((row_id, input_block_id));
            }
        }

        // PROBE PHASE
//        let mut join_result = Vec::new();

        for input_block_id in &self.block_rx_table_2 {
            let input_block = storage_manager.get_block(input_block_id).unwrap();
            let block_with_match = &mut input_block.clone();

            let row_ids = input_block.get_col(self.join_attribute_table_2).unwrap()
                .filter_map(|(input_row_id, key)|
                    hash_table.get(key).map(|key| (input_row_id, key))
                );

            for (input_row_id, matched_ids) in row_ids {
                let matched_row_ids = matched_ids.iter()
                    .map(|&(row_id, block_id)| (row_id, storage_manager.get_block(block_id).unwrap()))
                    .collect::<Vec<_>>();

                let mut rows = matched_row_ids.iter()
                    .map(|(matched_row_id, block_with_match)|
                        block_with_match.get_row(*matched_row_id).unwrap()
                            .chain(input_block.get_row(input_row_id).unwrap())

                    );

                util::send_rows(&mut rows, &self.block_tx, &self.router, storage_manager);
            }
        }
    }
}

#[cfg(test)]
mod hash_join_tests {
    use hustle_storage::StorageManager;
    use std::sync::mpsc;
    use std::mem;
    use crate::operator::hash_join::Hash_Join;

    #[test]
    fn test_hash_join() {
        let storage_manager = StorageManager::with_unique_data_directory();

        let (input_block_tx_1, input_block_rx_1) = mpsc::channel();
        let (input_block_tx_2, input_block_rx_2) = mpsc::channel();
        let (output_block_tx, output_block_rx) = mpsc::channel();

        // input_block_tx_1.send(block_1.id);
        // input_block_tx_2.send(block_2.id);

        mem::drop(input_block_tx_1);
        mem::drop(input_block_tx_2);

        let hash_join = Box::new(Hash_Join::new(router, input_block_rx_1, input_block_rx_2, output_block_tx, 0,0))

        let output_block_id = output_block_rx.recv().unwrap();

    }
}

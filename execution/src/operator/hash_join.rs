use std::sync::mpsc::{Receiver, Sender};

use hustle_catalog::Catalog;
use hustle_storage::StorageManager;

use crate::operator::{Operator, util};
use crate::router::BlockPoolDestinationRouter;

use std::collections::HashMap;

#[allow(unused)]
pub struct HashJoin {
    router: BlockPoolDestinationRouter,
    block_rx_table_1: Receiver<(u64)>,
    block_rx_table_2: Receiver<(u64)>,
    block_tx: Sender<u64>,
    join_attribute_table_1: usize,
    join_attribute_table_2: usize,
}

impl HashJoin {
    #[allow(unused)]
    pub fn new(
        router: BlockPoolDestinationRouter,
        block_rx_table_1: Receiver<u64>,
        block_rx_table_2: Receiver<u64>,
        block_tx: Sender<u64>,
        join_attribute_table_1: usize,
        join_attribute_table_2: usize,
    ) -> Self {
        HashJoin {
            router,
            block_rx_table_1,
            block_rx_table_2,
            block_tx,
            join_attribute_table_1,
            join_attribute_table_2,
        }
    }
}

impl Operator for HashJoin {
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
        for input_block_id in &self.block_rx_table_2 {
            let input_block = storage_manager.get_block(input_block_id).unwrap();

            // transform vector<(row id of table 1, block id of table 1)> to (row id of table 2, vector<(row id of table 1, block id of table 1)>)
            let row_ids = input_block.get_col(self.join_attribute_table_2).unwrap()
                .filter_map(|(input_row_id, key)|
                    hash_table.get(key).map(|value_by_hash_table| (input_row_id, value_by_hash_table))
                );

            // transform (input_row_id, vector<(row id, block id)> to (input_row_id, vector<(row id, actual block corresponding to that block id)>)
            for (input_row_id, value_by_hash_table) in row_ids {
                let matched_row_ids = value_by_hash_table.iter()
                    .map(|&(row_id, block_id)| (row_id, storage_manager.get_block(block_id).unwrap()))
                    .collect::<Vec<_>>();

                //let input_row = input_block.get_row(input_row_id).unwrap();

                let mut rows = matched_row_ids.iter()
                    .map(|(matched_row_id, block_with_match)|
                        block_with_match.get_row(*matched_row_id).unwrap()
                            .chain(input_block.get_row(input_row_id).unwrap())

                    );

                util::send_rows(&mut rows, &self.block_tx, &self.router, storage_manager)
            }
        }

        for block_id in self.router.get_all_block_ids() {
            self.block_tx.send(block_id).unwrap()
        }
    }
}


#[cfg(test)]
mod hash_join_tests {
    use hustle_storage::StorageManager;
    use std::sync::mpsc;
    use std::mem;
    use crate::operator::hash_join::HashJoin;
    use crate::router::BlockPoolDestinationRouter;
    use hustle_execution_test_util as test_util;
    use crate::operator::Operator;
    use super::*;

    #[test]
    fn test_hash_join() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let (input_block_tx_1, input_block_rx_1) = mpsc::channel();
        let (input_block_tx_2, input_block_rx_2) = mpsc::channel();
        let (output_block_tx, output_block_rx) = mpsc::channel();
        let block_1 = test_util::example_block(&storage_manager);
        let block_2 = test_util::example_block(&storage_manager);
        let catalog = Catalog::new();
        let mut table = test_util::example_table();
        let mut output_schema = table.columns.clone();
        output_schema.append(&mut table.columns);
        let router = BlockPoolDestinationRouter::new(output_schema);
        input_block_tx_1.send(block_1.id).unwrap();
        input_block_tx_2.send(block_2.id).unwrap();
        mem::drop(input_block_tx_1);
        mem::drop(input_block_tx_2);

        let hash_join = Box::new(HashJoin::new(router, input_block_rx_1, input_block_rx_2, output_block_tx, 1, 1));
        hash_join.execute(&storage_manager, &catalog);
        let output_block = storage_manager.get_block(output_block_rx.recv().unwrap()).unwrap();

        assert_eq!(output_block.get_row_col(0, 0), block_1.get_row_col(0, 0));
        assert_eq!(output_block.get_row_col(0, 1), block_1.get_row_col(0, 1));
        assert_eq!(output_block.get_row_col(0, 2), block_1.get_row_col(0, 2));

        assert_eq!(output_block.get_row_col(0, 3), block_2.get_row_col(0, 0));
        assert_eq!(output_block.get_row_col(0, 4), block_2.get_row_col(0, 1));
        assert_eq!(output_block.get_row_col(0, 5), block_2.get_row_col(0, 2));
        assert_eq!(output_block.get_row_col(0, 6), None);

        assert_eq!(output_block.get_row_col(1, 0), block_1.get_row_col(1, 0));
        assert_eq!(output_block.get_row_col(1, 1), block_1.get_row_col(1, 1));
        assert_eq!(output_block.get_row_col(1, 2), block_1.get_row_col(1, 2));

        assert_eq!(output_block.get_row_col(1, 3), block_2.get_row_col(1, 0));
        assert_eq!(output_block.get_row_col(1, 4), block_2.get_row_col(1, 1));
        assert_eq!(output_block.get_row_col(1, 5), block_2.get_row_col(1, 2));
        assert_eq!(output_block.get_row_col(1, 6), None);
        assert_eq!(output_block.get_row_col(2, 0), None);

        storage_manager.clear()
    }

    #[test]
    fn test_hash_join_with_more_rows() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let (input_block_tx_1, input_block_rx_1) = mpsc::channel();
        let (input_block_tx_2, input_block_rx_2) = mpsc::channel();
        let (output_block_tx, output_block_rx) = mpsc::channel();
        let block_1 = test_util::example_block_with_more_rows(&storage_manager);
        let block_2 = test_util::example_block_with_more_rows(&storage_manager);
        let catalog = Catalog::new();
        let mut table = test_util::example_table();
        let mut output_schema = table.columns.clone();
        output_schema.append(&mut table.columns);
        let router = BlockPoolDestinationRouter::new(output_schema);
        input_block_tx_1.send(block_1.id).unwrap();
        input_block_tx_2.send(block_2.id).unwrap();
        mem::drop(input_block_tx_1);
        mem::drop(input_block_tx_2);

        let hash_join = Box::new(HashJoin::new(router, input_block_rx_1, input_block_rx_2, output_block_tx, 0, 0));
        hash_join.execute(&storage_manager, &catalog);
        let output_block = storage_manager.get_block(output_block_rx.recv().unwrap()).unwrap();

        assert_eq!(output_block.get_row_col(0, 0), block_1.get_row_col(0, 0));
        assert_eq!(output_block.get_row_col(0, 1), block_1.get_row_col(0, 1));
        assert_eq!(output_block.get_row_col(0, 2), block_1.get_row_col(0, 2));

        assert_eq!(output_block.get_row_col(0, 3), block_2.get_row_col(0, 0));
        assert_eq!(output_block.get_row_col(0, 4), block_2.get_row_col(0, 1));
        assert_eq!(output_block.get_row_col(0, 5), block_2.get_row_col(0, 2));
        assert_eq!(output_block.get_row_col(0, 6), None);

        assert_eq!(output_block.get_row_col(1, 0), block_1.get_row_col(3, 0));
        assert_eq!(output_block.get_row_col(1, 1), block_1.get_row_col(3, 1));
        assert_eq!(output_block.get_row_col(1, 2), block_1.get_row_col(3, 2));

        assert_eq!(output_block.get_row_col(1, 3), block_2.get_row_col(0, 0));
        assert_eq!(output_block.get_row_col(1, 4), block_2.get_row_col(0, 1));
        assert_eq!(output_block.get_row_col(1, 5), block_2.get_row_col(0, 2));
        assert_eq!(output_block.get_row_col(1, 6), None);

        assert_eq!(output_block.get_row_col(2, 0), block_1.get_row_col(1, 0));
        assert_eq!(output_block.get_row_col(2, 1), block_1.get_row_col(1, 1));
        assert_eq!(output_block.get_row_col(2, 2), block_1.get_row_col(1, 2));

        assert_eq!(output_block.get_row_col(2, 3), block_2.get_row_col(1, 0));
        assert_eq!(output_block.get_row_col(2, 4), block_2.get_row_col(1, 1));
        assert_eq!(output_block.get_row_col(2, 5), block_2.get_row_col(1, 2));
        assert_eq!(output_block.get_row_col(2, 6), None);

        assert_eq!(output_block.get_row_col(3, 0), block_1.get_row_col(2, 0));
        assert_eq!(output_block.get_row_col(3, 1), block_1.get_row_col(2, 1));
        assert_eq!(output_block.get_row_col(3, 2), block_1.get_row_col(2, 2));

        assert_eq!(output_block.get_row_col(3, 3), block_2.get_row_col(1, 0));
        assert_eq!(output_block.get_row_col(3, 4), block_2.get_row_col(1, 1));
        assert_eq!(output_block.get_row_col(3, 5), block_2.get_row_col(1, 2));
        assert_eq!(output_block.get_row_col(3, 6), None);

        assert_eq!(output_block.get_row_col(4, 0), block_1.get_row_col(1, 0));
        assert_eq!(output_block.get_row_col(4, 1), block_1.get_row_col(1, 1));
        assert_eq!(output_block.get_row_col(4, 2), block_1.get_row_col(1, 2));

        assert_eq!(output_block.get_row_col(4, 3), block_2.get_row_col(2, 0));
        assert_eq!(output_block.get_row_col(4, 4), block_2.get_row_col(2, 1));
        assert_eq!(output_block.get_row_col(4, 5), block_2.get_row_col(2, 2));
        assert_eq!(output_block.get_row_col(4, 6), None);

        assert_eq!(output_block.get_row_col(5, 0), block_1.get_row_col(2, 0));
        assert_eq!(output_block.get_row_col(5, 1), block_1.get_row_col(2, 1));
        assert_eq!(output_block.get_row_col(5, 2), block_1.get_row_col(2, 2));

        assert_eq!(output_block.get_row_col(5, 3), block_2.get_row_col(2, 0));
        assert_eq!(output_block.get_row_col(5, 4), block_2.get_row_col(2, 1));
        assert_eq!(output_block.get_row_col(5, 5), block_2.get_row_col(2, 2));
        assert_eq!(output_block.get_row_col(5, 6), None);

        assert_eq!(output_block.get_row_col(6, 0), block_1.get_row_col(0, 0));
        assert_eq!(output_block.get_row_col(6, 1), block_1.get_row_col(0, 1));
        assert_eq!(output_block.get_row_col(6, 2), block_1.get_row_col(0, 2));

        assert_eq!(output_block.get_row_col(6, 3), block_2.get_row_col(3, 0));
        assert_eq!(output_block.get_row_col(6, 4), block_2.get_row_col(3, 1));
        assert_eq!(output_block.get_row_col(6, 5), block_2.get_row_col(3, 2));
        assert_eq!(output_block.get_row_col(6, 6), None);

        assert_eq!(output_block.get_row_col(7, 0), block_1.get_row_col(3, 0));
        assert_eq!(output_block.get_row_col(7, 1), block_1.get_row_col(3, 1));
        assert_eq!(output_block.get_row_col(7, 2), block_1.get_row_col(3, 2));

        assert_eq!(output_block.get_row_col(7, 3), block_2.get_row_col(3, 0));
        assert_eq!(output_block.get_row_col(7, 4), block_2.get_row_col(3, 1));
        assert_eq!(output_block.get_row_col(7, 5), block_2.get_row_col(3, 2));
        assert_eq!(output_block.get_row_col(7, 6), None);

        assert_eq!(output_block.get_row_col(8, 0), None);

        storage_manager.clear();
    }

}

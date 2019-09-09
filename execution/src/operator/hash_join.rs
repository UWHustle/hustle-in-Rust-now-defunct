use hustle_catalog::Catalog;
use hustle_storage::StorageManager;

use crate::operator::{Operator, util};
use crate::router::BlockPoolDestinationRouter;

use std::collections::HashMap;

pub struct Hash_Join {
    router: BlockPoolDestinationRouter,
    block_rx_table_1: Receiver<(usize, u64)>,
    block_rx_table_2: Receiver<(usize, u64)>,
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
    fn execute(&self, storage_manager: &StorageManager, _catalog: &Catalog) {
        let mut output_block = self.router.get_block(storage_manager);

        let mut hash_table = HashMap::new();

        // BUILD PHASE
        for input_block_id in &self.block_rx_table_1 {
            let input_block = storage_manager.get_block(input_block_id).unwrap();
            for row_id in input_block.row_ids() {
                let key = input_block.get_row_col(row_id, join_attribute_table_1);
                match key {
                    Some(&col_value) => {
                        let rows_with_col_value = hash_table.entry(col_value).or_insert(Vec::new());
                        *rows_with_col_value.push((row_id, input_block_id));
                    },
                }
            }
        }

        // PROBE PHASE
        for input_block_id in &self.block_rx_table_2 {
            let input_block = storage_manager.get_block(input_block_id).unwrap();
            let mut join_result: Vec<Vec<&[u8]>> = Vec::new();
            for input_row_id in input_block.row_ids() {
                let key = input_block.get_row_col(input_row_id, join_attribute_table_2);
                match key {
                    Some(&col_value) => {
                        let matched_result = hash_table.get(&col_value);
                        match matched_result {
                            Some(&matched_ids) => {
                                for (row_id, block_id) in matched_row_ids.iter() {
                                    let mut output_row = Vec::new();
                                    let block_with_match = storage_manager.get_block(block_id).unwrap();
                                    let n_cols_1 = block_with_match.n_cols();
                                    for i in 1..n_cols_1 {
                                        let find_entry = block_with_match.get_row_col(row_id, i);
                                        match find_entry {
                                            Some(&data) => output_row.push(data);
                                        }
                                    }
                                    let n_cols_2 = input_block.n_cols();
                                    for i in 1..n_cols_2 {
                                        let get_row_cols = input_block.get_row_col(input_row_id, i);
                                        match get_row_cols {
                                            Some(&my_data) => {}
                                            output_row.
                                            push(my_data);
                                        }
                                    }
                                    join_result.push(output_row);
                                }
                            }
                        }
                    }
                }
            }
            util::send_rows(&mut join_result.iter().map(|row| row.iter().map(|&buf| buf)), &mut output_block, &self.block_tx, &self.router, storage_manager)
        }
    }
}

#[cfg(test)]
mod hash_join_tests {

    #[test]
    fn test_hash_join() {

    }
}

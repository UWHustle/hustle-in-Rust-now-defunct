use std::sync::mpsc::{Receiver, Sender};

use hustle_catalog::Catalog;
use hustle_storage::StorageManager;

use crate::operator::{Operator, util};
use crate::router::BlockPoolDestinationRouter;

pub struct NestedJoin {
    router: BlockPoolDestinationRouter,
    block_rx_table_1: Receiver<(u64)>,
    block_rx_table_2: Receiver<(u64)>,
    block_rx_table_3: Receiver<(u64)>,
    block_tx: Sender<u64>,
    join_attribute_table_1: usize,
    join_attribute_table_2: usize,
    join_attribute_table_3: usize,
}

impl NestedJoin {
    pub fn new(
        router: BlockPoolDestinationRouter,
        block_rx_table_1: Receiver<u64>,
        block_rx_table_2: Receiver<u64>,
        block_rx_table_3: Receiver<u64>,
        block_tx: Sender<u64>,
        join_attribute_table_1: usize,
        join_attribute_table_2: usize,
        join_attribute_table_3: usize,
    ) -> Self {
        NestedJoin {
            router,
            block_rx_table_1,
            block_rx_table_2,
            block_rx_table_3,
            block_tx,
            join_attribute_table_1,
            join_attribute_table_2,
            join_attribute_table_3,
        }
    }
}

impl Operator for NestedJoin {
    fn execute(self: Box<Self>, storage_manager: &StorageManager, _catalog: &Catalog) {
        let mut ids_1 = Vec::new();
        let mut ids_2 = Vec::new();
        let mut ids_3 = Vec::new();

        for input_block_id in &self.block_rx_table_1 {
            ids_1.push(input_block_id.clone());
        }
        for input_block_id in &self.block_rx_table_2 {
            ids_2.push(input_block_id.clone());
        }
        for input_block_id in &self.block_rx_table_3 {
            ids_3.push(input_block_id.clone());
        }

        for input_block_id_1 in &ids_1 {
            for input_block_id_2 in &ids_2 {
                for input_block_id_3 in &ids_3 {
                    let mut matched_row_ids = Vec::new();
                   // let mut output_rows]
                    let input_block_1 = storage_manager.get_block(*input_block_id_1).unwrap();
                    let input_block_2 = storage_manager.get_block(*input_block_id_2).unwrap();
                    let input_block_3 = storage_manager.get_block(*input_block_id_3).unwrap();
                    for (row_id_1, col_value_1) in input_block_1.get_col(self.join_attribute_table_1).unwrap() {
                        for (row_id_2, col_value_2) in input_block_2.get_col(self.join_attribute_table_2).unwrap() {
                            for (row_id_3, col_value_3) in input_block_3.get_col(self.join_attribute_table_3).unwrap() {
                                if col_value_1 == col_value_2 && col_value_2 == col_value_3 {
                                    //let mut result = input_block_1.get_row(row_id_1).unwrap()
                                        //.chain(input_block_2.get_row(row_id_2).unwrap()).collect()
                                       // .chain(input_block_3.get_row(row_id_3).unwrap());
                                   // output_rows.push(result);
                                    matched_row_ids.push((row_id_1, row_id_2, row_id_3));
                                }
                            }
                        }
                    }

                    //let rows = output_rows.map(|row| row.into_iter().flat_map(|r| r.clone()));
                    let mut output_rows = matched_row_ids.iter()
                        .map(|(id_1, id_2, id_3)|
                            input_block_1.get_row(*id_1).unwrap()
                                .chain(input_block_2.get_row(*id_2).unwrap())
                                .chain(input_block_3.get_row(*id_3).unwrap())
                        );

                    util::send_rows(&mut output_rows, &self.block_tx, &self.router, storage_manager);
                }
            }
        }

        for block_id in self.router.get_all_block_ids() {
            self.block_tx.send(block_id).unwrap()
        }
    }
}

#[cfg(test)]
mod nested_loop_tests {
    use hustle_storage::StorageManager;
    use std::sync::mpsc;
    use std::mem;
    use crate::operator::nested_loop_join::NestedJoin;
    use crate::router::BlockPoolDestinationRouter;
    use hustle_execution_test_util as test_util;
    use crate::operator::Operator;
    use super::*;

    #[test]
    fn test_nested_join() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let (input_block_tx_1, input_block_rx_1) = mpsc::channel();
        let (input_block_tx_2, input_block_rx_2) = mpsc::channel();
        let (input_block_tx_3, input_block_rx_3) = mpsc::channel();
        let (output_block_tx, output_block_rx) = mpsc::channel();

        let block_1 = test_util::example_block(&storage_manager);
        let block_2 = test_util::example_block(&storage_manager);
        let block_3 = test_util::example_block(&storage_manager);

        let catalog = Catalog::new();
        let mut table = test_util::example_table();
        let mut output_schema = table.columns.clone();
        output_schema.append(&mut table.columns.clone());
        output_schema.append(&mut table.columns);
        let router = BlockPoolDestinationRouter::new(output_schema);

        input_block_tx_1.send(block_1.id);
        input_block_tx_2.send(block_2.id);
        input_block_tx_3.send(block_3.id);
        mem::drop(input_block_tx_1);
        mem::drop(input_block_tx_2);
        mem::drop(input_block_tx_3);

        let nested_join = Box::new(NestedJoin::new(router, input_block_rx_1, input_block_rx_2, input_block_rx_3, output_block_tx, 1, 1, 1));
        nested_join.execute(&storage_manager, &catalog);
        let output_block = storage_manager.get_block(output_block_rx.recv().unwrap()).unwrap();

        assert_eq!(output_block.get_row_col(0, 0), block_1.get_row_col(0, 0));
        assert_eq!(output_block.get_row_col(0, 1), block_1.get_row_col(0, 1));
        assert_eq!(output_block.get_row_col(0, 2), block_1.get_row_col(0, 2));

        assert_eq!(output_block.get_row_col(0, 3), block_2.get_row_col(0, 0));
        assert_eq!(output_block.get_row_col(0, 4), block_2.get_row_col(0, 1));
        assert_eq!(output_block.get_row_col(0, 5), block_2.get_row_col(0, 2));

        assert_eq!(output_block.get_row_col(0, 6), block_3.get_row_col(0, 0));
        assert_eq!(output_block.get_row_col(0, 7), block_3.get_row_col(0, 1));
        assert_eq!(output_block.get_row_col(0, 8), block_3.get_row_col(0, 2));
        assert_eq!(output_block.get_row_col(0, 9), None);

        assert_eq!(output_block.get_row_col(1, 0), block_1.get_row_col(1, 0));
        assert_eq!(output_block.get_row_col(1, 1), block_1.get_row_col(1, 1));
        assert_eq!(output_block.get_row_col(1, 2), block_1.get_row_col(1, 2));

        assert_eq!(output_block.get_row_col(1, 3), block_2.get_row_col(1, 0));
        assert_eq!(output_block.get_row_col(1, 4), block_2.get_row_col(1, 1));
        assert_eq!(output_block.get_row_col(1, 5), block_2.get_row_col(1, 2));

        assert_eq!(output_block.get_row_col(1, 6), block_3.get_row_col(1, 0));
        assert_eq!(output_block.get_row_col(1, 7), block_3.get_row_col(1, 1));
        assert_eq!(output_block.get_row_col(1, 8), block_3.get_row_col(1, 2));
        assert_eq!(output_block.get_row_col(1, 9), None);

        assert_eq!(output_block.get_row_col(2, 0), None);

    }
}
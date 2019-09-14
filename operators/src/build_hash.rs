use crate::{evaluate_predicate, WorkOrder};
use hustle_common::{JoinContext, JoinHashTable};

#[derive(Debug)]
pub struct BuildHash {
    op_index: usize,
    join_context: JoinContext,
    join_hash_table: JoinHashTable,
    num_workorders_generated: usize,
    input_block_ids: Vec<usize>,
    input_relation_is_stored: bool,
    started: bool,
    done_feeding_input: bool,
}

impl BuildHash {
    pub fn new(
        op_index: usize,
        join_context: JoinContext,
        join_hash_table: JoinHashTable,
        input_block_ids: Vec<usize>,
        input_relation_is_stored: bool,
    ) -> Self {
        BuildHash {
            op_index,
            join_context,
            join_hash_table,
            num_workorders_generated: 0,
            input_block_ids,
            input_relation_is_stored,
            started: false,
            done_feeding_input: false,
        }
    }
}

impl super::Operator for BuildHash {
    fn get_all_work_orders(
        &mut self,
        work_orders_container: &mut crate::WorkOrdersContainer,
    ) -> bool {
        if self.input_relation_is_stored {
            if self.started {
                return true;
            }

            let num_blocks = self.input_block_ids.len();
            work_orders_container.reserve(self.op_index, num_blocks);

            let mut work_orders: Vec<Box<dyn WorkOrder>> = Vec::with_capacity(num_blocks);
            for block in &self.input_block_ids {
                work_orders.push(Box::new(BuildHashWorkOrder::new(
                    self.join_context.clone(),
                    self.join_hash_table.clone(),
                    *block,
                )));
            }
            work_orders_container.add_normal_work_orders(self.op_index, work_orders);
            self.started = true;

            true
        } else {
            // FIXME
            unimplemented!()
            // self.done_feeding_input
        }
    }

    fn feed_input(&mut self, _input_feed: Option<super::InputFeed>) {
        unimplemented!()
    }

    fn done_feeding_input(&mut self, _relation_name: Option<String>) {
        self.done_feeding_input = true;
    }
}

pub struct BuildHashWorkOrder {
    join_context: JoinContext,
    join_hash_table: JoinHashTable,
    input_block_id: usize,
}

impl BuildHashWorkOrder {
    pub fn new(
        join_context: JoinContext,
        join_hash_table: JoinHashTable,
        input_block_id: usize,
    ) -> Self {
        BuildHashWorkOrder {
            join_context,
            join_hash_table,
            input_block_id,
        }
    }
}

impl WorkOrder for BuildHashWorkOrder {
    fn execute(
        &mut self,
        storage_manager: std::sync::Arc<hustle_storage::StorageManager>,
        _lookup: &mut Vec<u32>,
    ) {
        let re = storage_manager.relational_engine();
        let block = re
            .get_block(self.join_context.table_name.as_str(), self.input_block_id)
            .unwrap();

        match &self.join_hash_table {
            JoinHashTable::Existence(existence) => {
                for rid in 0..block.get_n_rows() {
                    if let Some(predicate) = &self.join_context.predicate {
                        if !evaluate_predicate(predicate, &block, rid, &mut None) {
                            continue;
                        }
                    }

                    // FIXME: Assume i32
                    let join_column = unsafe {
                        *(block
                            .get_row_col(rid, self.join_context.join_column_id)
                            .unwrap()
                            .as_ptr() as *const i32)
                    };
                    {
                        let mut data = existence[join_column as usize].lock().unwrap();
                        *data = true;
                    }
                }
            }
            JoinHashTable::PrimaryIntKeyIntPayload(lookup_table) => {
                for rid in 0..block.get_n_rows() {
                    if let Some(predicate) = &self.join_context.predicate {
                        if !evaluate_predicate(predicate, &block, rid, &mut None) {
                            continue;
                        }
                    }

                    // FIXME: Assume i32
                    let join_column = unsafe {
                        *(block
                            .get_row_col(rid, self.join_context.join_column_id)
                            .unwrap()
                            .as_ptr() as *const i32)
                    };

                    debug_assert_eq!(1, self.join_context.payload_columns.len());
                    let payload_column = self.join_context.payload_columns.first().unwrap();
                    let payload = unsafe {
                        *(block
                            .get_row_col(rid, payload_column.id())
                            .unwrap()
                            .as_ptr() as *const i32)
                    };

                    {
                        let mut data = lookup_table[join_column as usize].lock().unwrap();
                        *data = payload;
                    }
                }
            }
            JoinHashTable::PrimaryIntKeyStringPayload(lookup_table) => {
                for rid in 0..block.get_n_rows() {
                    if let Some(predicate) = &self.join_context.predicate {
                        if !evaluate_predicate(predicate, &block, rid, &mut None) {
                            continue;
                        }
                    }

                    // FIXME: Assume i32
                    let join_column = unsafe {
                        *(block
                            .get_row_col(rid, self.join_context.join_column_id)
                            .unwrap()
                            .as_ptr() as *const i32)
                    };

                    debug_assert_eq!(1, self.join_context.payload_columns.len());
                    let payload_column = self.join_context.payload_columns.first().unwrap();
                    let payload_column_bytes = block.get_row_col(rid, payload_column.id()).unwrap();
                    let payload = std::str::from_utf8(payload_column_bytes).unwrap();

                    {
                        let mut data = lookup_table[join_column as usize].lock().unwrap();
                        *data = payload.to_string();
                    }
                }
            }
        };
    }
}

use crate::{InputFeed, WorkOrder, WorkOrdersContainer};
use hustle_storage::StorageManager;
use std::rc::Rc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

pub struct CountTriangles {
    op_index: usize,
    input_table_name: String,
    row_info: Rc<Vec<(usize, (usize, usize))>>,
    block_rows: Vec<usize>,
    num_rows: usize,
    triangle_count: Arc<AtomicI64>,
}

impl CountTriangles {
    pub fn new(
        op_index: usize,
        input_table_name: String,
        row_info: &Vec<(usize, (usize, usize))>,
        block_rows: &Vec<usize>,
        num_rows: usize,
        triangle_count: Arc<AtomicI64>,
    ) -> Self {
        CountTriangles {
            op_index,
            input_table_name,
            row_info: Rc::new(row_info.clone()),
            block_rows: block_rows.clone(),
            num_rows,
            triangle_count,
        }
    }
}

impl super::Operator for CountTriangles {
    fn get_all_work_orders(&mut self, work_orders_container: &mut WorkOrdersContainer) -> bool {
        let num_blocks = self.block_rows.len();
        work_orders_container.reserve(self.op_index, num_blocks);

        let mut work_orders: Vec<Box<dyn WorkOrder>> = Vec::with_capacity(num_blocks);
        for u in &self.block_rows {
            let u = *u;
            if u == 0 {
                continue;
            }
            work_orders.push(Box::new(CountTrianglesWorkOrder::new(
                self.input_table_name.clone(),
                Rc::clone(&self.row_info),
                u,
                self.num_rows,
                Arc::clone(&self.triangle_count),
            )));
        }

        work_orders_container.add_normal_work_orders(self.op_index, work_orders);

        true
    }

    fn feed_input(&mut self, input_feed: Option<InputFeed>) {
        unimplemented!()
    }

    fn done_feeding_input(&mut self, relation_name: Option<String>) {
        unimplemented!()
    }
}

pub struct CountTrianglesWorkOrder {
    input_table_name: String,
    row_info: Rc<Vec<(usize, (usize, usize))>>,
    row_id: usize,
    num_rows: usize,
    block_id: usize,
    triangle_count: Arc<AtomicI64>,
}

impl CountTrianglesWorkOrder {
    pub fn new(
        input_table_name: String,
        row_info: Rc<Vec<(usize, (usize, usize))>>,
        row_id: usize,
        num_rows: usize,
        triangle_count: Arc<AtomicI64>,
    ) -> Self {
        let block_id = (row_info[row_id].1).0;
        CountTrianglesWorkOrder {
            input_table_name,
            row_info,
            row_id,
            num_rows,
            block_id,
            triangle_count,
        }
    }

    fn count_triangle(
        &mut self,
        storage_manager: &Arc<StorageManager>,
        lookup: &mut Vec<u32>,
        u: usize,
        count: &mut i64,
    ) {
        let u_info = self.row_info[u];
        let num_rows = u_info.0;
        debug_assert!(num_rows > 1);
        debug_assert_eq!(self.block_id, (u_info.1).0);

        let re = storage_manager.relational_engine();
        let block = re
            .get_block(self.input_table_name.as_str(), self.block_id)
            .unwrap();

        let block_rows = block.get_n_rows();
        let rid = (u_info.1).1;

        let (rid_end, extra_blocks) = if rid + num_rows <= block_rows {
            (rid + num_rows, false)
        } else {
            (block_rows, true)
        };

        for r in rid..rid_end {
            let bytes = block.get_row_col(r, 0).unwrap();
            lookup[unsafe { *(bytes.as_ptr() as *const i32) } as usize] = u as u32;
        }

        if extra_blocks {
            let mut num_rows_left = num_rows + rid - block_rows;
            let mut block_id = self.block_id + 1;
            while num_rows_left >= block_rows {
                num_rows_left -= block_rows;
                let block = re
                    .get_block(self.input_table_name.as_str(), block_id)
                    .unwrap();
                for r in 0..block_rows {
                    let bytes = block.get_row_col(r, 0).unwrap();
                    lookup[unsafe { *(bytes.as_ptr() as *const i32) } as usize] = u as u32;
                }
                block_id += 1;
            }

            let block = re
                .get_block(self.input_table_name.as_str(), block_id)
                .unwrap();
            for r in 0..num_rows_left {
                let bytes = block.get_row_col(r, 0).unwrap();
                lookup[unsafe { *(bytes.as_ptr() as *const i32) } as usize] = u as u32;
            }
            for r in 0..num_rows_left {
                let bytes = block.get_row_col(r, 0).unwrap();
                let v = unsafe { *(bytes.as_ptr() as *const i32) } as usize;
                Self::count_triangle_helper(self, storage_manager, lookup, u, v, block_rows, count);
            }

            let mut num_rows_left = num_rows + rid - block_rows;
            let mut block_id = self.block_id + 1;
            while num_rows_left >= block_rows {
                num_rows_left -= block_rows;
                let block = re
                    .get_block(self.input_table_name.as_str(), block_id)
                    .unwrap();
                for r in 0..block_rows {
                    let bytes = block.get_row_col(r, 0).unwrap();
                    let v = unsafe { *(bytes.as_ptr() as *const i32) } as usize;
                    Self::count_triangle_helper(
                        self,
                        storage_manager,
                        lookup,
                        u,
                        v,
                        block_rows,
                        count,
                    );
                }
                block_id += 1;
            }
        }

        for r in rid..rid_end {
            let bytes = block.get_row_col(r, 0).unwrap();
            let v = unsafe { *(bytes.as_ptr() as *const i32) } as usize;
            Self::count_triangle_helper(self, storage_manager, lookup, u, v, block_rows, count);
        }
    }

    fn count_triangle_helper(
        &self,
        storage_manager: &Arc<StorageManager>,
        lookup: &Vec<u32>,
        u: usize,
        v: usize,
        block_rows: usize,
        count: &mut i64,
    ) {
        debug_assert!(u < v);
        let v_info = self.row_info[v];
        let v_num_rows = v_info.0;
        if v_num_rows < 1 {
            return;
        }

        let (v_block_id, v_rid) = v_info.1;
        let re = storage_manager.relational_engine();
        let v_block = re
            .get_block(self.input_table_name.as_str(), v_block_id)
            .unwrap();

        let (v_rid_end, v_extra_blocks) = if v_rid + v_num_rows <= block_rows {
            (v_rid + v_num_rows, false)
        } else {
            (block_rows, true)
        };

        for r in v_rid..v_rid_end {
            let w =
                unsafe { *(v_block.get_row_col(r, 0).unwrap().as_ptr() as *const i32) } as usize;
            debug_assert!(v < w);
            if lookup[w] == u as u32 {
                *count += 1;
            }
        }

        if v_extra_blocks {
            let mut v_num_rows_left = v_num_rows + v_rid - block_rows;
            let mut block_id = v_block_id + 1;
            while v_num_rows_left >= block_rows {
                v_num_rows_left -= block_rows;
                let block = re
                    .get_block(self.input_table_name.as_str(), block_id)
                    .unwrap();
                for r in 0..block_rows {
                    let bytes = block.get_row_col(r, 0).unwrap();
                    let w = unsafe { *(block.get_row_col(r, 0).unwrap().as_ptr() as *const i32) }
                        as usize;
                    debug_assert!(v < w);
                    if lookup[w] == u as u32 {
                        *count += 1;
                    }
                }
                block_id += 1;
            }

            let block = re
                .get_block(self.input_table_name.as_str(), block_id)
                .unwrap();
            for r in 0..v_num_rows_left {
                let bytes = block.get_row_col(r, 0).unwrap();
                let w =
                    unsafe { *(block.get_row_col(r, 0).unwrap().as_ptr() as *const i32) } as usize;
                debug_assert!(v < w);
                if lookup[w] == u as u32 {
                    *count += 1;
                }
            }
        }
    }
}

impl WorkOrder for CountTrianglesWorkOrder {
    fn execute(&mut self, storage_manager: Arc<StorageManager>, lookup: &mut Vec<u32>) {
        if lookup.is_empty() {
            lookup.resize(self.num_rows + 1, 0);
        }

        let mut count = 0i64;

        let mut row = self.row_id;
        debug_assert!(row > 0);
        while row <= self.num_rows {
            let row_info = self.row_info[row];
            if row_info.0 < 2 {
                row += 1;
            } else if (row_info.1).0 == self.block_id {
                Self::count_triangle(self, &storage_manager, lookup, row, &mut count);
                row += 1;
            } else {
                break;
            }
        }

        self.triangle_count.fetch_add(count, Ordering::Relaxed);
    }
}

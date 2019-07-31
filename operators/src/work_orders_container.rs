use crate::WorkOrder;
use std::collections::VecDeque;

pub struct WorkOrdersContainer {
    normal_work_orders: Vec<OperatorWorkOrdersContainer>,
    normal_work_order_op_index: VecDeque<(usize, usize)>,
    num_pending_normal_work_orders: Vec<usize>,
    // rebuild_work_orders: Vec<OperatorWorkOrdersContainer>,
}

impl WorkOrdersContainer {
    pub fn new(num_operators: usize) -> Self {
        WorkOrdersContainer {
            normal_work_orders: (0..num_operators)
                .map(|_| OperatorWorkOrdersContainer::new())
                .collect(),
            normal_work_order_op_index: VecDeque::new(),
            num_pending_normal_work_orders: vec![0usize; num_operators],
            // rebuild_work_orders: : (0..num_operators).map(|_| OperatorWorkOrdersContainer::new()).collect(),
        }
    }

    pub fn reserve(&mut self, op_index: usize, num_work_orders: usize) {
        debug_assert!(op_index < self.normal_work_orders.len());
        self.normal_work_orders[op_index].reserve(num_work_orders);
    }

    pub fn add_normal_work_order(&mut self, op_index: usize, work_order: Box<dyn WorkOrder>) {
        Self::add_normal_work_orders(self, op_index, vec![work_order]);
    }

    pub fn add_normal_work_orders(
        &mut self,
        op_index: usize,
        work_orders: Vec<Box<dyn WorkOrder>>,
    ) {
        debug_assert!(op_index < self.normal_work_orders.len());

        let num_work_orders = work_orders.len();

        for work_order in work_orders {
            self.normal_work_orders[op_index].add_work_order(work_order);
        }

        let mut work_order_from_new_op_index = true;
        if let Some(last_work_order_info) = self.normal_work_order_op_index.back_mut() {
            if last_work_order_info.0 == op_index {
                last_work_order_info.1 += num_work_orders;
                work_order_from_new_op_index = false;
            }
        }

        if work_order_from_new_op_index {
            self.normal_work_order_op_index
                .push_back((op_index, num_work_orders));
        }
    }

    pub fn has_normal_work_order(&self, op_index: usize) -> bool {
        self.normal_work_orders[op_index].has_work_order()
            || self.num_pending_normal_work_orders[op_index] != 0
    }

    pub fn get_num_normal_work_orders(&self, op_index: usize) -> usize {
        debug_assert!(op_index < self.normal_work_orders.len());
        self.normal_work_orders[op_index].get_num_work_orders()
    }

    pub fn get_next_normal_work_orders(&mut self) -> Vec<(Box<dyn WorkOrder>, usize)> {
        let mut work_orders = vec![];
        while !self.normal_work_order_op_index.is_empty() {
            let first_work_order_info = self.normal_work_order_op_index.front_mut().unwrap();
            let op_index = first_work_order_info.0;
            let num_work_orders = first_work_order_info.1;
            self.normal_work_order_op_index.pop_front();

            self.num_pending_normal_work_orders[op_index] += num_work_orders;
            work_orders.reserve(num_work_orders);
            for _i in 0..num_work_orders {
                work_orders.push((
                    self.normal_work_orders[op_index]
                        .get_next_work_order()
                        .unwrap(),
                    op_index,
                ));
            }
        }

        work_orders
    }

    pub fn mark_work_order_completion(&mut self, op_index: usize) {
        self.num_pending_normal_work_orders[op_index] -= 1;
    }

    /*
    pub fn add_rebuild_work_order(&mut self, op_index: usize, work_order: Box<WorkOrder>) {
        debug_assert!(op_index < self.normal_work_orders.len());
        self.rebuild_work_orders[op_index].add_work_order(work_order);
    }

    pub fn has_rebuild_work_order(&self, op_index: usize) -> bool {
        debug_assert!(op_index < self.normal_work_orders.len());
        self.rebuild_work_orders[op_index].has_work_order()
    }
    */
}

struct OperatorWorkOrdersContainer {
    work_orders: VecDeque<Box<dyn WorkOrder>>,
}

impl OperatorWorkOrdersContainer {
    pub fn new() -> Self {
        OperatorWorkOrdersContainer {
            work_orders: VecDeque::new(),
        }
    }

    pub fn reserve(&mut self, num_work_orders: usize) {
        self.work_orders.reserve(num_work_orders);
    }

    pub fn add_work_order(&mut self, work_order: Box<dyn WorkOrder>) {
        self.work_orders.push_back(work_order);
    }

    pub fn has_work_order(&self) -> bool {
        !self.work_orders.is_empty()
    }

    pub fn get_num_work_orders(&self) -> usize {
        self.work_orders.len()
    }

    pub fn get_next_work_order(&mut self) -> Option<Box<dyn WorkOrder>> {
        self.work_orders.pop_front()
    }
}

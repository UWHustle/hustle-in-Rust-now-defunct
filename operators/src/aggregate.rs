use crate::{evaluate_predicate, evaluate_scalar, WorkOrder};
use hustle_common::{AggregateContext, AggregateFunction, AggregateState, Literal, Predicate};

use std::sync::Arc;

use hustle_storage::StorageManager;
use std::sync::atomic::Ordering;

// #[derive(Debug)]
pub struct Aggregate {
    op_index: usize,
    input_table_name: String,
    aggregate_context: Vec<AggregateContext>,
    aggregate_state: AggregateState,
    filter: Option<Predicate>,
    num_workorders_generated: usize,
    input_block_ids: Vec<usize>,
    input_relation_is_stored: bool,
    started: bool,
    done_feeding_input: bool,
}

impl Aggregate {
    pub fn new(
        op_index: usize,
        input_table_name: String,
        aggregate_context: Vec<AggregateContext>,
        aggregate_state: AggregateState,
        filter: Option<Predicate>,
        input_block_ids: Vec<usize>,
        input_relation_is_stored: bool,
    ) -> Self {
        Aggregate {
            op_index,
            input_table_name,
            aggregate_context,
            aggregate_state,
            filter,
            num_workorders_generated: 0,
            input_block_ids,
            input_relation_is_stored,
            started: false,
            done_feeding_input: false,
        }
    }
}

impl super::Operator for Aggregate {
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
                work_orders.push(Box::new(AggregateWorkOrder {
                    aggregate_context: self.aggregate_context.clone(),
                    aggregate_state: self.aggregate_state.clone(),
                    filter: self.filter.clone(),
                    input_table_name: self.input_table_name.clone(),
                    input_block_id: block.clone(),
                }));
            }
            work_orders_container.add_normal_work_orders(self.op_index, work_orders);
            self.started = true;

            true
        } else {
            while self.num_workorders_generated < self.input_block_ids.len() {
                let work_order = Box::new(AggregateWorkOrder {
                    aggregate_context: self.aggregate_context.clone(),
                    aggregate_state: self.aggregate_state.clone(),
                    filter: self.filter.clone(),
                    input_table_name: self.input_table_name.clone(),
                    input_block_id: self.input_block_ids[self.num_workorders_generated].clone(),
                });
                work_orders_container.add_normal_work_order(self.op_index, work_order);
                self.num_workorders_generated += 1;
            }

            self.done_feeding_input
        }
    }

    fn feed_input(&mut self, input_feed: Option<super::InputFeed>) {
        if let Some(input_feed) = input_feed {
            match input_feed {
                super::InputFeed::Block(block_id, ..) => {
                    self.input_block_ids.push(block_id);
                }
            }
        }
    }

    fn done_feeding_input(&mut self, _relation_name: Option<String>) {
        self.done_feeding_input = true;
    }
}

pub struct AggregateWorkOrder {
    aggregate_context: Vec<AggregateContext>,
    aggregate_state: AggregateState,
    filter: Option<Predicate>,
    input_table_name: String,
    input_block_id: usize,
}

impl AggregateWorkOrder {
    pub fn new(
        aggregate_context: Vec<AggregateContext>,
        aggregate_state: AggregateState,
        filter: Option<Predicate>,
        input_table_name: String,
        input_block_id: usize,
    ) -> Self {
        AggregateWorkOrder {
            aggregate_context,
            aggregate_state,
            filter,
            input_table_name,
            input_block_id,
        }
    }
}

impl WorkOrder for AggregateWorkOrder {
    fn execute(&mut self, storage_manager: Arc<StorageManager>, _lookup: &mut Vec<u32>) {
        let re = storage_manager.relational_engine();
        let block = re
            .get_block(self.input_table_name.as_str(), self.input_block_id)
            .unwrap();

        let num_aggr = self.aggregate_context.len();
        match &self.aggregate_state {
            AggregateState::SingleStates(states) => {
                let mut aggr = vec![0i64; num_aggr];
                for rid in 0..block.get_n_rows() {
                    if let Some(predicate) = &self.filter {
                        if !evaluate_predicate(predicate, &block, rid, &mut None) {
                            continue;
                        }
                    }

                    for i in 0..num_aggr {
                        let aggr_context = &self.aggregate_context[i];
                        let literal = evaluate_scalar(&aggr_context.argument, &block, rid);
                        let v;
                        if let Literal::Int32(i) = literal {
                            v = i as i64;
                        } else {
                            panic!("TODO");
                        }

                        match aggr_context.function {
                            AggregateFunction::Sum => {
                                aggr[i] += v;
                            }
                        }
                    }
                }

                for i in 0..num_aggr {
                    states[i].fetch_add(aggr[i], Ordering::Relaxed);
                }
            }
            _ => unimplemented!(),
        };
    }
}

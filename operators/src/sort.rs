use crate::{WorkOrder, WorkOrdersContainer};
use hustle_common::{get_flip_literal, AggregateState, Literal, OutputSource, SortInput};
use hustle_storage::StorageManager;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

// #[derive(Debug)]
pub struct Sort {
    op_index: usize,
    input: SortInput,
    keys: Vec<(OutputSource, bool)>,
    payloads: Vec<OutputSource>,
    limit: Option<usize>,
    output: Arc<Mutex<BTreeMap<Vec<Literal>, Vec<Literal>>>>,
    started: bool,
    done_feeding_input: bool,
}

impl Sort {
    pub fn new(
        op_index: usize,
        input: SortInput,
        keys: &Vec<(OutputSource, bool)>,
        payloads: Vec<OutputSource>,
        limit: Option<usize>,
        output: Arc<Mutex<BTreeMap<Vec<Literal>, Vec<Literal>>>>,
    ) -> Self {
        Sort {
            op_index,
            input,
            keys: keys.clone(),
            payloads,
            limit,
            output,
            started: false,
            done_feeding_input: false,
        }
    }
}

impl super::Operator for Sort {
    fn get_all_work_orders(&mut self, work_orders_container: &mut WorkOrdersContainer) -> bool {
        if self.started {
            return true;
        }

        let work_order: Box<dyn WorkOrder> = Box::new(SortWorkOrder::new(
            self.input.clone(),
            self.keys.clone(),
            self.payloads.clone(),
            self.output.clone(),
        ));
        work_orders_container.add_normal_work_order(self.op_index, work_order);

        self.started = true;
        true
    }

    fn feed_input(&mut self, _input_feed: Option<super::InputFeed>) {
        unimplemented!()
    }

    fn done_feeding_input(&mut self, _relation_name: Option<String>) {
        self.done_feeding_input = true;
    }
}

// #[derive(Debug)]
pub struct SortWorkOrder {
    input: SortInput,
    keys: Vec<(OutputSource, bool)>,
    payloads: Vec<OutputSource>,
    output: Arc<Mutex<BTreeMap<Vec<Literal>, Vec<Literal>>>>,
}

impl SortWorkOrder {
    pub fn new(
        input: SortInput,
        keys: Vec<(OutputSource, bool)>,
        payloads: Vec<OutputSource>,
        output: Arc<Mutex<BTreeMap<Vec<Literal>, Vec<Literal>>>>,
    ) -> Self {
        SortWorkOrder {
            input,
            keys,
            payloads,
            output,
        }
    }
}

impl super::WorkOrder for SortWorkOrder {
    fn execute(&mut self, _storage_manager: Arc<StorageManager>, _lookup: &mut Vec<u32>) {
        match &self.input {
            SortInput::Aggregate(aggrgate_state) => {
                match aggrgate_state {
                    AggregateState::GroupByStatesDashMap(hash_table) => {
                        for r in hash_table.iter() {
                            let keys = r.key();
                            let payload = *r.value();
                            let mut ordered_key = Vec::with_capacity(self.keys.len());
                            for (data_source, flip) in &self.keys {
                                let literal = match data_source {
                                    OutputSource::Key(i) => {
                                        if *flip {
                                            get_flip_literal(&keys[*i])
                                        } else {
                                            keys[*i].clone()
                                        }
                                    }
                                    OutputSource::Payload(_) => {
                                        if *flip {
                                            Literal::Int64Reverse(Reverse(payload))
                                        } else {
                                            Literal::Int64(payload)
                                        }
                                    }
                                };
                                ordered_key.push(literal);
                            }

                            let mut ordered_payload = Vec::with_capacity(self.payloads.len());
                            for data_source in &self.payloads {
                                let literal: Literal = match data_source {
                                    OutputSource::Key(i) => keys[*i].clone(),
                                    OutputSource::Payload(_) => Literal::Int64(payload),
                                };
                                ordered_payload.push(literal);
                            }

                            {
                                let mut output = self.output.lock().unwrap();
                                output.insert(ordered_key, ordered_payload);
                            }
                        }
                    }
                    AggregateState::SingleStates(_) => unreachable!(),
                };
            }
        }
    }
}

use crate::{evaluate_predicate, evaluate_scalar, WorkOrder};
use hustle_common::{
    AggregateContext, AggregateState, DataSource, EvaluationOrder, JoinHashTable, JoinHashTables,
    Literal, Predicate, Scalar,
};
use hustle_storage::relational_block::RelationalBlock;
use hustle_storage::StorageManager;
use std::collections::HashMap;
use std::ops::DerefMut;
use std::rc::Rc;
use std::sync::atomic::Ordering;

// #[derive(Debug)]
pub struct StarJoinAggregate {
    op_index: usize,
    probe_table_name: String,
    probe_filter: Option<Predicate>,
    probe_join_column_ids: Vec<usize>,
    probe_non_group_by_join_column_ids: Vec<usize>,
    join_hash_tables: JoinHashTables,
    groups: Vec<DataSource>,
    aggregate_context: AggregateContext,
    aggregate_state: AggregateState,
    evaluation_order: Rc<Vec<EvaluationOrder>>,
    groups_from_base_relations: Rc<Vec<usize>>,
    probe_block_ids: Vec<usize>,
    probe_relation_is_stored: bool,
    started: bool,
    done_feeding_input: usize,
}

impl StarJoinAggregate {
    pub fn new(
        op_index: usize,
        probe_table_name: &str,
        probe_filter: Option<Predicate>,
        probe_join_column_ids: Vec<usize>,
        probe_non_group_by_join_column_ids: Vec<usize>,
        join_hash_tables: JoinHashTables,
        groups: Vec<DataSource>,
        aggregate_context: AggregateContext,
        aggregate_state: AggregateState,
        evaluation_order: Rc<Vec<EvaluationOrder>>,
        groups_from_base_relations: Rc<Vec<usize>>,
        probe_block_ids: Vec<usize>,
        probe_relation_is_stored: bool,
    ) -> Self {
        let done_feeding_input = probe_join_column_ids.len();
        StarJoinAggregate {
            op_index,
            probe_table_name: probe_table_name.to_owned(),
            probe_filter,
            probe_join_column_ids,
            probe_non_group_by_join_column_ids,
            join_hash_tables,
            groups,
            aggregate_context,
            aggregate_state,
            evaluation_order,
            groups_from_base_relations,
            probe_block_ids,
            probe_relation_is_stored,
            started: false,
            done_feeding_input,
        }
    }
}

impl super::Operator for StarJoinAggregate {
    fn get_all_work_orders(
        &mut self,
        work_orders_container: &mut crate::WorkOrdersContainer,
    ) -> bool {
        if self.probe_relation_is_stored {
            debug_assert_eq!(0, self.done_feeding_input);
            if self.started {
                return true;
            }

            let num_blocks = self.probe_block_ids.len();
            work_orders_container.reserve(self.op_index, num_blocks);
            let mut work_orders: Vec<Box<dyn WorkOrder>> = Vec::with_capacity(num_blocks);
            for block in &self.probe_block_ids {
                work_orders.push(Box::new(StarJoinAggregateWorkOrder::new(
                    self.probe_table_name.clone(),
                    *block,
                    self.probe_filter.clone(),
                    self.probe_join_column_ids.clone(),
                    self.probe_non_group_by_join_column_ids.clone(),
                    self.join_hash_tables.clone(),
                    self.groups.clone(),
                    self.aggregate_context.clone(),
                    self.aggregate_state.clone(),
                    Rc::clone(&self.evaluation_order),
                    Rc::clone(&self.groups_from_base_relations),
                )));
            }
            work_orders_container.add_normal_work_orders(self.op_index, work_orders);
            self.started = true;

            true
        } else {
            unimplemented!()
        }
    }

    fn feed_input(&mut self, _input_feed: Option<super::InputFeed>) {
        unimplemented!()
    }

    fn done_feeding_input(&mut self, _relation_name: Option<String>) {
        self.done_feeding_input -= 1;
    }
}

pub struct StarJoinAggregateWorkOrder {
    probe_table_name: String,
    probe_block_id: usize,
    probe_filter: Option<Predicate>,
    probe_join_column_ids: Vec<usize>,
    probe_non_group_by_join_column_ids: Vec<usize>,
    join_hash_tables: JoinHashTables,
    groups: Vec<DataSource>,
    aggregate_context: AggregateContext,
    aggregate_state: AggregateState,
    evaluation_order: Rc<Vec<EvaluationOrder>>,
    groups_from_base_relations: Rc<Vec<usize>>,
}

impl StarJoinAggregateWorkOrder {
    pub fn new(
        probe_table_name: String,
        probe_block_id: usize,
        probe_filter: Option<Predicate>,
        probe_join_column_ids: Vec<usize>,
        probe_non_group_by_join_column_ids: Vec<usize>,
        join_hash_tables: JoinHashTables,
        groups: Vec<DataSource>,
        aggregate_context: AggregateContext,
        aggregate_state: AggregateState,
        evaluation_order: Rc<Vec<EvaluationOrder>>,
        groups_from_base_relations: Rc<Vec<usize>>,
    ) -> Self {
        StarJoinAggregateWorkOrder {
            probe_table_name,
            probe_block_id,
            probe_filter,
            probe_join_column_ids,
            probe_non_group_by_join_column_ids,
            join_hash_tables,
            groups,
            aggregate_context,
            aggregate_state,
            evaluation_order,
            groups_from_base_relations,
        }
    }

    // #[inline(always)]
    fn execute_aggregate_opt(
        &self,
        block: &RelationalBlock,
        rid: usize,
    ) -> Option<(Vec<Literal>, i64)> {
        debug_assert!(!self.evaluation_order.is_empty());

        let mut keys = vec![Literal::Int32(0); self.groups.len()];
        for order in self.evaluation_order.iter() {
            match order {
                EvaluationOrder::BaseRelation => {
                    debug_assert!(self.probe_filter.is_some());
                    let predicate = self.probe_filter.as_ref().unwrap();
                    if !evaluate_predicate(predicate, &block, rid, &mut None) {
                        return None;
                    }
                }
                EvaluationOrder::ExistenceJoin(i) => {
                    let non_group_by_join_column_id = self.probe_join_column_ids[*i];
                    let bytes = block.get_row_col(rid, non_group_by_join_column_id).unwrap();
                    let lhs = unsafe { *(bytes.as_ptr() as *const i32) };
                    match &self.join_hash_tables.join_hash_tables[*i] {
                        JoinHashTable::Existence(existence) => {
                            if !*existence[lhs as usize].lock().unwrap() {
                                return None;
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                EvaluationOrder::GroupByIndex(i) => {
                    let i = *i;
                    keys[i] = match &self.groups[i] {
                        DataSource::BaseRelation(scalar) => {
                            debug_assert!(self.probe_filter.is_some());
                            let predicate = self.probe_filter.as_ref().unwrap();
                            let mut literal = Literal::Int32(0);
                            if !evaluate_predicate(predicate, &block, rid, &mut Some(&mut literal))
                            {
                                return None;
                            }
                            literal.clone()
                        }
                        DataSource::JoinHashTable(j) => {
                            let j = *j;
                            let bytes = block
                                .get_row_col(rid, self.probe_join_column_ids[j])
                                .unwrap();
                            let lhs = unsafe { *(bytes.as_ptr() as *const i32) };
                            let literal = match &self.join_hash_tables.join_hash_tables[j] {
                                JoinHashTable::PrimaryIntKeyIntPayload(lookup_table) => {
                                    let rhs = *&*lookup_table[lhs as usize].lock().unwrap();
                                    if rhs == 0 {
                                        return None;
                                    }

                                    Literal::Int32(rhs)
                                }
                                JoinHashTable::PrimaryIntKeyStringPayload(lookup_table) => {
                                    let rhs = &*lookup_table[lhs as usize].lock().unwrap();
                                    if rhs.is_empty() {
                                        return None;
                                    }

                                    Literal::Char(rhs.clone())
                                }
                                _ => unreachable!(),
                            };
                            literal
                        }
                    };
                }
            }
        }

        for i in self.groups_from_base_relations.iter() {
            let i = *i;
            if let DataSource::BaseRelation(scalar) = &self.groups[i] {
                keys[i] = evaluate_scalar(scalar, block, rid);
            } else {
                unreachable!()
            }
        }

        let aggr;
        let literal = evaluate_scalar(&self.aggregate_context.argument, block, rid);
        if let Literal::Int32(i) = literal {
            aggr = i as i64;
        } else {
            panic!("TODO");
        }

        Some((keys, aggr))
    }

    // #[inline(always)]
    fn execute_aggregate(
        &self,
        block: &RelationalBlock,
        rid: usize,
    ) -> Option<(Vec<Literal>, i64)> {
        for non_group_by_join_column_index in &self.probe_non_group_by_join_column_ids {
            let non_group_by_join_column_id =
                self.probe_join_column_ids[*non_group_by_join_column_index];
            let bytes = block.get_row_col(rid, non_group_by_join_column_id).unwrap();
            let lhs = unsafe { *(bytes.as_ptr() as *const i32) };
            match &self.join_hash_tables.join_hash_tables[*non_group_by_join_column_index] {
                JoinHashTable::Existence(existence) => {
                    if !*existence[lhs as usize].lock().unwrap() {
                        return None;
                    }
                }
                _ => unreachable!(),
            }
        }

        if let Some(predicate) = &self.probe_filter {
            if !evaluate_predicate(predicate, &block, rid, &mut None) {
                return None;
            }
        }

        let mut keys = Vec::with_capacity(self.groups.len());
        for group in &self.groups {
            match group {
                DataSource::BaseRelation(scalar) => {
                    keys.push(evaluate_scalar(scalar, block, rid));
                }
                DataSource::JoinHashTable(index) => {
                    let index = *index;
                    let bytes = block
                        .get_row_col(rid, self.probe_join_column_ids[index])
                        .unwrap();
                    let lhs = unsafe { *(bytes.as_ptr() as *const i32) };
                    match &self.join_hash_tables.join_hash_tables[index] {
                        JoinHashTable::PrimaryIntKeyIntPayload(lookup_table) => {
                            let rhs = *&*lookup_table[lhs as usize].lock().unwrap();
                            if rhs == 0 {
                                return None;
                            }

                            keys.push(Literal::Int32(rhs));
                        }
                        JoinHashTable::PrimaryIntKeyStringPayload(lookup_table) => {
                            let rhs = &*lookup_table[lhs as usize].lock().unwrap();
                            if rhs.is_empty() {
                                return None;
                            }

                            // keys.push(Literal::Char(rhs.clone()));
                            keys.push(Literal::Char(rhs.trim_end().to_string()));
                        }
                        _ => unreachable!(),
                    }
                }
            }
        }

        let aggr;
        let literal = evaluate_scalar(&self.aggregate_context.argument, block, rid);
        if let Literal::Int32(i) = literal {
            aggr = i as i64;
        } else {
            panic!("TODO");
        }

        Some((keys, aggr))
    }
}

impl super::WorkOrder for StarJoinAggregateWorkOrder {
    fn execute(&mut self, storage_manager: std::sync::Arc<StorageManager>) {
        let re = storage_manager.relational_engine();
        let block = re
            .get_block(self.probe_table_name.as_str(), self.probe_block_id)
            .unwrap();

        unsafe {}
        match &self.aggregate_state {
            AggregateState::SingleStates(states) => {
                debug_assert_eq!(1usize, states.len());
                let mut local_aggr = 0i64;
                if self.evaluation_order.is_empty() {
                    for rid in 0..block.get_n_rows() {
                        if let Some((_, aggr)) = Self::execute_aggregate(self, &block, rid) {
                            local_aggr += aggr;
                        }
                    }
                } else {
                    for rid in 0..block.get_n_rows() {
                        if let Some((_, aggr)) = Self::execute_aggregate_opt(self, &block, rid) {
                            local_aggr += aggr;
                        }
                    }
                }

                states[0].fetch_add(local_aggr, Ordering::Relaxed);
            }
            AggregateState::GroupByStatesDashMap(hash_table) => {
                if self.evaluation_order.is_empty() {
                    for rid in 0..block.get_n_rows() {
                        if let Some((keys, aggr)) = Self::execute_aggregate(self, &block, rid) {
                            if let Some(mut r) = hash_table.get_mut(&keys) {
                                *r.deref_mut() += aggr;
                            } else {
                                hash_table.insert(keys, aggr);
                            }
                        }
                    }
                } else {
                    for rid in 0..block.get_n_rows() {
                        if let Some((keys, aggr)) = Self::execute_aggregate_opt(self, &block, rid) {
                            if let Some(mut r) = hash_table.get_mut(&keys) {
                                *r.deref_mut() += aggr;
                            } else {
                                hash_table.insert(keys, aggr);
                            }
                        }
                    }
                }
            }
            AggregateState::GroupByStatesInt(min_value, _const_groups, ordered_table) => {
                for rid in 0..block.get_n_rows() {
                    if let Some((keys, aggr)) = Self::execute_aggregate(self, &block, rid) {
                        debug_assert_eq!(1, keys.len());
                        if let Literal::Int32(i) = keys.first().unwrap() {
                            debug_assert!(*i >= *min_value);
                            ordered_table[(*i - *min_value) as usize]
                                .fetch_add(aggr, Ordering::Relaxed);
                        } else {
                            unreachable!()
                        }
                    }
                }
            }
        }
    }
}

extern crate dashmap;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::AtomicI64;
use std::sync::{Arc, Mutex};

use hustle_common::{
    AggregateState, ColumnType, DataSource, JoinHashTable, JoinHashTables, Literal, OutputSource,
    PhysicalPlan, QueryResult, SortInput,
};
use hustle_operators::build_hash::BuildHash;
use hustle_operators::{
    Aggregate, CountTriangles, QueryPlan, QueryPlanDag, Sort, StarJoinAggregate,
};
use hustle_storage::StorageManager;

use dashmap::DashMap;
use hustle_common::PhysicalPlan::TriangleCounting;
use std::rc::Rc;

#[derive(Clone)]
enum OperatorOuputInfo {
    Relation(String, Option<usize>),
    Aggregate(usize, Vec<OutputSource>, AggregateState),
    Sort(
        usize,
        Vec<OutputSource>,
        Arc<Mutex<BTreeMap<Vec<Literal>, Vec<Literal>>>>,
    ),
    SingleValue(usize, Arc<AtomicI64>),
}

pub struct ExecutionGenerator {
    storage_manager: Arc<StorageManager>,
    physical_to_output_relation_map: HashMap<PhysicalPlan, OperatorOuputInfo>,
}

impl ExecutionGenerator {
    pub fn new(storage_manager: Arc<StorageManager>) -> Self {
        ExecutionGenerator {
            storage_manager,
            physical_to_output_relation_map: HashMap::new(),
        }
    }

    pub fn generate_plan(
        &mut self,
        physical_plan: &PhysicalPlan,
        query_plan: &mut QueryPlan,
        query_plan_dag: &mut QueryPlanDag,
    ) {
        if let PhysicalPlan::TopLevelPlan { plan, .. } = physical_plan {
            self.physical_to_output_relation_map.clear();
            Self::generate_plan_helper(self, plan, query_plan, query_plan_dag);

            if query_plan.query_id != 0 {
                return;
            }

            let (query_result, schema) =
                match &self.physical_to_output_relation_map.get(plan).unwrap() {
                    OperatorOuputInfo::Aggregate(_, schema, aggregate_state) => (
                        QueryResult::Aggregate(aggregate_state.clone()),
                        schema.clone(),
                    ),
                    OperatorOuputInfo::Sort(_, schema, ordered_table) => {
                        (QueryResult::Sort(ordered_table.clone()), schema.clone())
                    }
                    OperatorOuputInfo::SingleValue(_, atomic) => {
                        (QueryResult::SingleValue(atomic.clone()), vec![])
                    }
                    _ => unreachable!(),
                };

            query_plan.set_query_result(query_result, schema);
        } else {
            panic!("Ill-formed plan");
        }
    }

    fn generate_plan_helper(
        &mut self,
        plan: &PhysicalPlan,
        query_plan: &mut QueryPlan,
        query_plan_dag: &mut QueryPlanDag,
    ) {
        match plan {
            PhysicalPlan::Aggregate {
                table,
                aggregates,
                groups,
                filter,
                output_schema,
            } => {
                Self::generate_plan_helper(self, table, query_plan, query_plan_dag);
                match self.physical_to_output_relation_map.get(&*table).unwrap() {
                    OperatorOuputInfo::Relation(input_table, producer_op_index) => {
                        let re = self.storage_manager.relational_engine();
                        debug_assert!(re.exists(input_table));
                        let input_physical_relation = re.get(input_table).unwrap();
                        let block_ids = input_physical_relation.block_ids();

                        let aggr_index = query_plan.num_operators();
                        let aggregate_state;
                        if groups.is_empty() {
                            aggregate_state = AggregateState::SingleStates(Arc::new(
                                (0..aggregates.len())
                                    .map(|_| AtomicI64::new(0))
                                    .collect::<Vec<AtomicI64>>(),
                            ));
                        } else {
                            aggregate_state =
                                AggregateState::GroupByStatesDashMap(Arc::new(DashMap::default()));
                        }
                        let aggr_op = Box::new(Aggregate::new(
                            aggr_index,
                            input_table.clone(),
                            aggregates.clone(),
                            aggregate_state.clone(),
                            filter.clone(),
                            block_ids.clone(),
                            producer_op_index.is_none(),
                        ));
                        query_plan.add_operator(aggr_op);
                        query_plan_dag.add_node();

                        if let Some(input_index) = producer_op_index {
                            query_plan_dag.add_direct_dependency(aggr_index, *input_index);
                        }

                        self.physical_to_output_relation_map.insert(
                            plan.clone(),
                            OperatorOuputInfo::Aggregate(
                                aggr_index,
                                output_schema.clone(),
                                aggregate_state.clone(),
                            ),
                        );
                    }
                    _ => unreachable!(),
                };
            }
            PhysicalPlan::StarJoinAggregate {
                fact_table,
                fact_table_filter,
                fact_table_join_column_ids,
                dim_tables,
                aggregate_context,
                groups,
                evaluation_order,
                groups_from_fact_table,
                output_schema,
            } => {
                let re = self.storage_manager.relational_engine();

                let num_dim_tables = dim_tables.len();
                let mut build_hash_op_indices = Vec::with_capacity(num_dim_tables);
                let mut join_hash_tables = Vec::with_capacity(num_dim_tables);
                for i in 0..num_dim_tables {
                    let dim_table = &dim_tables[i];

                    let join_hash_table;
                    let max_count = dim_table.max_count;
                    let payload_columns = &dim_table.payload_columns;
                    if payload_columns.is_empty() {
                        join_hash_table = JoinHashTable::Existence(Arc::new(
                            (0..max_count)
                                .map(|_| Mutex::new(false))
                                .collect::<Vec<Mutex<bool>>>(),
                        ));
                    } else if payload_columns.len() == 1 {
                        let payload_column = payload_columns.first().unwrap();
                        join_hash_table = match payload_column.column__type {
                            ColumnType::I32 => JoinHashTable::PrimaryIntKeyIntPayload(Arc::new(
                                (0..max_count)
                                    .map(|_| Mutex::new(0))
                                    .collect::<Vec<Mutex<i32>>>(),
                            )),
                            ColumnType::Char(_) | ColumnType::VarChar(_) => {
                                JoinHashTable::PrimaryIntKeyStringPayload(Arc::new(
                                    (0..max_count)
                                        .map(|_| Mutex::new(String::from("")))
                                        .collect::<Vec<Mutex<String>>>(),
                                ))
                            }
                            _ => unimplemented!(),
                        };
                    } else {
                        unimplemented!()
                    }

                    let input_table_name = dim_table.table_name.as_str();
                    debug_assert!(re.exists(input_table_name));
                    let input_physical_relation = re.get(input_table_name).unwrap();
                    let block_ids = input_physical_relation.block_ids();
                    let build_hash_index = query_plan.num_operators();
                    let build_hash_op = Box::new(BuildHash::new(
                        build_hash_index,
                        dim_table.clone(),
                        join_hash_table.clone(),
                        block_ids.clone(),
                        true,
                    ));
                    query_plan.add_operator(build_hash_op);
                    query_plan_dag.add_node();

                    join_hash_tables.push(join_hash_table);
                    build_hash_op_indices.push(build_hash_index);
                }

                Self::generate_plan_helper(self, fact_table, query_plan, query_plan_dag);
                match self
                    .physical_to_output_relation_map
                    .get(&*fact_table)
                    .unwrap()
                {
                    OperatorOuputInfo::Relation(input_table, producer_op_index) => {
                        let re = self.storage_manager.relational_engine();
                        debug_assert!(re.exists(input_table));
                        let input_physical_relation = re.get(input_table).unwrap();
                        let block_ids = input_physical_relation.block_ids();

                        let aggr_index = query_plan.num_operators();
                        let aggregate_state;
                        if groups.is_empty() {
                            aggregate_state =
                                AggregateState::SingleStates(Arc::new(vec![AtomicI64::new(0)]));
                        } else {
                            aggregate_state =
                                AggregateState::GroupByStatesDashMap(Arc::new(DashMap::default()));
                        }

                        let mut group_by_join_column_ids = HashSet::new();
                        for group in groups {
                            if let DataSource::JoinHashTable(i) = group {
                                group_by_join_column_ids.insert(*i);
                            }
                        }

                        let mut input_non_group_by_join_column_ids = vec![];
                        for i in 0..fact_table_join_column_ids.len() {
                            if !group_by_join_column_ids.contains(&i) {
                                input_non_group_by_join_column_ids.push(i);
                            }
                        }

                        let aggr_op = Box::new(StarJoinAggregate::new(
                            aggr_index,
                            input_table.as_str(),
                            fact_table_filter.clone(),
                            fact_table_join_column_ids.clone(),
                            input_non_group_by_join_column_ids,
                            JoinHashTables::new(join_hash_tables),
                            groups.clone(),
                            aggregate_context.clone(),
                            aggregate_state.clone(),
                            Rc::new(evaluation_order.clone()),
                            Rc::new(groups_from_fact_table.clone()),
                            block_ids.clone(),
                            producer_op_index.is_none(),
                        ));
                        query_plan.add_operator(aggr_op);
                        query_plan_dag.add_node();

                        if let Some(input_index) = producer_op_index {
                            query_plan_dag.add_direct_dependency(aggr_index, *input_index);
                        }

                        for index in build_hash_op_indices {
                            query_plan_dag.add_direct_dependency(aggr_index, index);
                        }

                        self.physical_to_output_relation_map.insert(
                            plan.clone(),
                            OperatorOuputInfo::Aggregate(
                                aggr_index,
                                output_schema.clone(),
                                aggregate_state.clone(),
                            ),
                        );
                    }
                    _ => unreachable!(),
                };
            }
            PhysicalPlan::TableReference { table, .. } => {
                self.physical_to_output_relation_map.insert(
                    plan.clone(),
                    OperatorOuputInfo::Relation(table.name.clone(), None),
                );
            }
            PhysicalPlan::TriangleCounting {
                input,
                row_info,
                block_row,
                num_rows,
            } => {
                Self::generate_plan_helper(self, input, query_plan, query_plan_dag);
                match self.physical_to_output_relation_map.get(&*input).unwrap() {
                    OperatorOuputInfo::Relation(input_table, producer_op_index) => {
                        let tc_index = query_plan.num_operators();
                        let count = Arc::new(AtomicI64::new(0));
                        let tc_op = Box::new(CountTriangles::new(
                            tc_index,
                            input_table.clone(),
                            row_info,
                            block_row,
                            *num_rows,
                            Arc::clone(&count),
                        ));
                        query_plan.add_operator(tc_op);
                        query_plan_dag.add_node();

                        self.physical_to_output_relation_map.insert(
                            plan.clone(),
                            OperatorOuputInfo::SingleValue(tc_index, Arc::clone(&count)),
                        );
                    }
                    _ => unimplemented!(),
                }
            }
            PhysicalPlan::Sort {
                input,
                sort_attributes,
                limit,
                output_schema,
            } => {
                Self::generate_plan_helper(self, input, query_plan, query_plan_dag);
                match &self.physical_to_output_relation_map.get(&*input).unwrap() {
                    OperatorOuputInfo::Aggregate(producer_op_index, schema, aggregate_state) => {
                        let op_index = query_plan.num_operators();
                        let mut sort_keys = HashSet::new();
                        for sort_attribute in sort_attributes {
                            sort_keys.insert(sort_attribute.0.clone());
                        }

                        let mut sort_payloads =
                            Vec::with_capacity(schema.len() - sort_attributes.len());
                        for column in schema {
                            if !sort_keys.contains(column) {
                                sort_payloads.push(column.clone());
                            }
                        }

                        let ordered_table = Arc::new(Mutex::new(BTreeMap::new()));
                        let sort_op = Box::new(Sort::new(
                            op_index,
                            SortInput::Aggregate(aggregate_state.clone()),
                            sort_attributes,
                            sort_payloads,
                            limit.clone(),
                            ordered_table.clone(),
                        ));
                        query_plan.add_operator(sort_op);
                        query_plan_dag.add_node();
                        query_plan_dag.add_direct_dependency(op_index, *producer_op_index);

                        self.physical_to_output_relation_map.insert(
                            plan.clone(),
                            OperatorOuputInfo::Sort(op_index, output_schema.clone(), ordered_table),
                        );
                    }
                    _ => unimplemented!(),
                }
            }
            _ => unimplemented!(),
        }
    }
}

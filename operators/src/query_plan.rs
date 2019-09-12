use crate::Operator;

use hustle_common::{AggregateState, Literal, OutputSource, QueryResult};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;

#[derive(Debug)]
pub struct QueryPlanDag {
    dependents: Vec<HashSet<usize>>,
}

impl QueryPlanDag {
    pub fn new() -> Self {
        QueryPlanDag {
            dependents: Vec::new(),
        }
    }

    pub fn add_node(&mut self) {
        self.dependents.push(HashSet::new());
    }

    pub fn add_direct_dependency(&mut self, consumer_op_index: usize, producer_op_index: usize) {
        debug_assert!(consumer_op_index < self.dependents.len());
        debug_assert!(producer_op_index < self.dependents.len());

        self.dependents[producer_op_index].insert(consumer_op_index);
    }

    pub fn dependents(&self) -> &Vec<HashSet<usize>> {
        &self.dependents
    }
}

// #[derive(Debug)]
pub struct QueryPlan {
    pub query_id: usize,
    operators: Vec<Box<dyn Operator>>,
    query_result: Option<QueryResult>,
    output_schema: Vec<OutputSource>,
}

impl QueryPlan {
    pub fn new(query_id: usize) -> Self {
        QueryPlan {
            query_id,
            operators: Vec::new(),
            query_result: None,
            output_schema: Vec::new(),
        }
    }

    pub fn add_operator(&mut self, operator: Box<dyn Operator>) -> usize {
        let index = self.operators.len();
        self.operators.push(operator);
        index
    }

    pub fn num_operators(&self) -> usize {
        self.operators.len()
    }

    pub fn get_operator_mutable(&mut self, op_index: usize) -> &mut dyn Operator {
        debug_assert!(op_index < self.operators.len());

        self.operators[op_index].as_mut()
    }

    pub fn set_query_result(
        &mut self,
        query_result: QueryResult,
        output_schema: Vec<OutputSource>,
    ) {
        self.query_result = Some(query_result);
        self.output_schema = output_schema;
    }

    pub fn display(&self) {
        let num_columns = self.output_schema.len();
        if let Some(query_result) = &self.query_result {
            match query_result {
                QueryResult::Aggregate(aggregate_state) => match aggregate_state {
                    AggregateState::SingleStates(states) => {
                        debug_assert_eq!(num_columns, states.len());
                        for i in 0..states.len() {
                            if i != 0 {
                                print!(" ");
                            }

                            print!("{}", states[i].load(Ordering::Relaxed));
                        }
                        println!();
                    }
                    AggregateState::GroupByStatesInt(int_min_value, constant_keys, payloads) => {
                        let mut key_indexes = HashMap::new();
                        for (i, literal) in constant_keys {
                            key_indexes.insert(*i, literal.clone());
                        }

                        for i in 0..payloads.len() {
                            for j in 0..num_columns {
                                if j != 0 {
                                    print!(" ");
                                }

                                match &self.output_schema[j] {
                                    OutputSource::Key(k) => {
                                        if *k == 0 {
                                            print!("{}", i as i32 + *int_min_value);
                                        } else if let Some(literal) = key_indexes.get(k) {
                                            print_literal(literal);
                                        } else {
                                            unreachable!()
                                        }
                                    }
                                    OutputSource::Payload(_) => {
                                        print!("{}", payloads[i].load(Ordering::Relaxed));
                                    }
                                }
                            }
                            println!();
                        }
                    }
                    AggregateState::GroupByStatesDashMap(hash_table) => {
                        for r in hash_table.iter() {
                            for i in 0..num_columns {
                                if i != 0 {
                                    print!(" ");
                                }

                                match &self.output_schema[i] {
                                    OutputSource::Key(j) => {
                                        print_literal(&r.key()[*j]);
                                    }
                                    OutputSource::Payload(_) => {
                                        print!("{}", r.value());
                                    }
                                }
                            }
                        }
                    }
                },
                QueryResult::Sort(ordered_table) => {
                    let ordered_table = &*ordered_table.lock().unwrap();
                    for (key, payload) in ordered_table {
                        for i in 0..num_columns {
                            if i != 0 {
                                print!(" ");
                            }

                            match &self.output_schema[i] {
                                OutputSource::Key(j) => {
                                    print_literal(&key[*j]);
                                }
                                OutputSource::Payload(j) => {
                                    print_literal(&payload[*j]);
                                }
                            }
                        }
                        println!();
                    }
                }
            }
        }
    }
}

fn print_literal(literal: &Literal) {
    match literal {
        Literal::Int32(i) => print!("{}", i),
        Literal::Int64(i) | Literal::Int64Reverse(std::cmp::Reverse(i)) => print!("{}", i),
        Literal::Char(str) => print!("{}", str.trim_end()),
        _ => unimplemented!(),
    }
}

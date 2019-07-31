use hustle_common::{
    get_referenced_attribute, get_referenced_attributes, get_relation_size, BinaryOperation,
    Comparison, DataSource, Database, EvaluationOrder, JoinContext, Literal, PhysicalPlan,
    Predicate, Scalar,
};
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(PartialEq, PartialOrd)]
struct NonNan(f64);

impl Eq for NonNan {}

impl Ord for NonNan {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

enum Transformer {
    IntYyyyMmDd,
    StringLastChar,
    StringLastFourThreeLastTwoOne,
    StringLastTwoCharsMultiply,
}

pub struct SetEvaluationOrder {
    stat_info: HashMap<
        &'static str,
        (
            Option<Literal>, /* min */
            Option<Literal>, /* max */
            usize,           /* cardinality */
            Option<Transformer>,
        ),
    >,
}

impl SetEvaluationOrder {
    pub fn new() -> Self {
        // FIXME: get the stat info from database
        let mut stat_info = HashMap::new();
        stat_info.insert(
            "c_city",
            (
                Some(Literal::Int32(0)),
                Some(Literal::Int32(9)),
                10 * 25,
                Some(Transformer::StringLastChar),
            ),
        );
        stat_info.insert("c_nation", (None, None, 25, None));
        stat_info.insert("c_region", (None, None, 5, None));
        stat_info.insert(
            "lo_discount",
            (Some(Literal::Int32(0)), Some(Literal::Int32(10)), 11, None),
        );
        stat_info.insert(
            "lo_orderdate",
            (
                Some(Literal::Int32(19920101)),
                Some(Literal::Int32(19981231)),
                0,
                Some(Transformer::IntYyyyMmDd),
            ),
        );
        stat_info.insert(
            "lo_quantity",
            (Some(Literal::Int32(1)), Some(Literal::Int32(50)), 50, None),
        );
        stat_info.insert(
            "p_brand1",
            (
                Some(Literal::Int32(1)),
                Some(Literal::Int32(40)),
                1000,
                Some(Transformer::StringLastFourThreeLastTwoOne),
            ),
        );
        stat_info.insert(
            "p_category",
            (
                Some(Literal::Int32(1)),
                Some(Literal::Int32(25)),
                25,
                Some(Transformer::StringLastTwoCharsMultiply),
            ),
        );
        stat_info.insert(
            "p_mfgr",
            (
                Some(Literal::Int32(1)),
                Some(Literal::Int32(5)),
                5,
                Some(Transformer::StringLastChar),
            ),
        );
        stat_info.insert(
            "s_city",
            (
                Some(Literal::Int32(0)),
                Some(Literal::Int32(9)),
                10 * 25,
                Some(Transformer::StringLastChar),
            ),
        );
        stat_info.insert("s_nation", (None, None, 25, None));
        stat_info.insert("s_region", (None, None, 5, None));

        SetEvaluationOrder { stat_info }
    }

    fn calculate_scalar_stat(
        &self,
        scalar: &Scalar,
        base_relation_scalar_index: &HashMap<Scalar, usize>,
        scalar_index: &mut Option<&mut i32>,
    ) -> (Option<Literal>, Option<Literal>, usize) {
        if let Some(i) = scalar_index {
            if **i == -1 {
                if let Some(index) = base_relation_scalar_index.get(scalar) {
                    **i = *index as i32;
                }
            }
        }

        match scalar {
            Scalar::ScalarLiteral(literal) => (Some(literal.clone()), Some(literal.clone()), 1),
            Scalar::ScalarAttribute(c) => {
                let stat = self.stat_info.get(c.name.as_str());
                debug_assert!(stat.is_some());
                let stat = stat.unwrap();
                (stat.0.clone(), stat.1.clone(), stat.2)
            }
            Scalar::BinaryExpression {
                operation,
                left,
                right,
            } => {
                let (left_min, left_max, _) = Self::calculate_scalar_stat(
                    self,
                    &**left,
                    base_relation_scalar_index,
                    scalar_index,
                );
                let left_min = get_literal_int32(&left_min.unwrap());
                let left_max = get_literal_int32(&left_max.unwrap());

                let referenced_attributes = get_referenced_attribute(&**left);
                debug_assert_eq!(1, referenced_attributes.len());
                let stat = self
                    .stat_info
                    .get(referenced_attributes.iter().nth(0).unwrap().name.as_str());
                debug_assert!(stat.is_some());
                let stat = stat.unwrap();
                let transformer = &stat.3;

                let (right_min, right_max, _) = Self::calculate_scalar_stat(
                    self,
                    &**right,
                    base_relation_scalar_index,
                    scalar_index,
                );
                let right_min = get_literal_int32(&right_min.unwrap());
                let right_max = get_literal_int32(&right_max.unwrap());
                debug_assert_eq!(right_min, right_max);
                match operation {
                    BinaryOperation::Divide => {
                        let cardinality = if let Some(t) = transformer {
                            match t {
                                Transformer::IntYyyyMmDd => {
                                    if right_min == 100 {
                                        7 * 12
                                    } else if right_max == 10000 {
                                        7
                                    } else {
                                        unreachable!()
                                    }
                                }
                                _ => unimplemented!(),
                            }
                        } else {
                            0
                        };
                        (
                            Some(Literal::Int32(left_min / right_min)),
                            Some(Literal::Int32(left_max / right_min)),
                            cardinality,
                        )
                    }
                    _ => unimplemented!(),
                }
            }
            Scalar::UnaryExpression { .. } => unimplemented!(),
        }
    }

    fn calculate_predicate_selectivity(
        &self,
        predicate: &Predicate,
        base_relation_scalar_index: &HashMap<Scalar, usize>,
        scalar_index: &mut Option<&mut i32>,
    ) -> f64 {
        match predicate {
            Predicate::True | Predicate::False | Predicate::Conjunction { .. } => unreachable!(),
            Predicate::Between {
                operand,
                begin,
                end,
            } => match &**operand {
                Scalar::ScalarLiteral(_) => unreachable!(),
                Scalar::ScalarAttribute(column) => {
                    let stat = self.stat_info.get(column.name.as_str());
                    debug_assert!(stat.is_some());
                    let stat = stat.unwrap();
                    if let Some(transformer) = &stat.3 {
                        match transformer {
                            Transformer::StringLastFourThreeLastTwoOne => {
                                let cardinality = stat.2 as f64;

                                let begin_value;
                                let last_four_three;
                                if let Scalar::ScalarLiteral(Literal::Char(str)) = &**begin {
                                    begin_value = parse_string_last_two_one(str);
                                    last_four_three = parse_string_last_four_three(str);
                                } else {
                                    unimplemented!();
                                }

                                let end_value;
                                if let Scalar::ScalarLiteral(Literal::Char(str)) = &**end {
                                    end_value = parse_string_last_two_one(str);
                                    debug_assert_eq!(
                                        last_four_three,
                                        parse_string_last_four_three(str)
                                    );
                                } else {
                                    unimplemented!();
                                }

                                return ((end_value - begin_value + 1) as f64) / cardinality;
                            }
                            _ => unimplemented!(),
                        }
                    } else {
                        debug_assert!(stat.0.is_some());
                        let min = get_literal_int32(&stat.0.as_ref().unwrap());

                        debug_assert!(stat.1.is_some());
                        let max = get_literal_int32(&stat.1.as_ref().unwrap());

                        let begin_value;
                        if let Scalar::ScalarLiteral(Literal::Int32(i)) = &**begin {
                            begin_value = *i;
                        } else {
                            unimplemented!();
                        }

                        let end_value;
                        if let Scalar::ScalarLiteral(Literal::Int32(i)) = &**end {
                            end_value = *i;
                        } else {
                            unimplemented!();
                        }

                        return ((end_value - begin_value + 1) as f64) / ((max - min) as f64);
                    }
                }
                Scalar::UnaryExpression { .. } | Scalar::BinaryExpression { .. } => {
                    unimplemented!()
                }
            },
            Predicate::Comparison {
                comparison,
                left,
                right,
            } => {
                if let Some(i) = scalar_index {
                    if **i == -1 {
                        if let Some(index) = base_relation_scalar_index.get(&**left) {
                            **i = *index as i32;
                        }
                    }
                }

                let (min, max, cardinality) = Self::calculate_scalar_stat(
                    self,
                    &**left,
                    base_relation_scalar_index,
                    scalar_index,
                );

                match comparison {
                    Comparison::Equal => {
                        if cardinality != 0 {
                            1.0 / cardinality as f64
                        } else {
                            let min = get_literal_int32(&min.unwrap());
                            let max = get_literal_int32(&max.unwrap());
                            1.0 / (max - min + 1) as f64
                        }
                    }
                    Comparison::NotEqual => {
                        let mut not_predicate = predicate.clone();
                        if let Predicate::Comparison { comparison, .. } = &mut not_predicate {
                            *comparison = Comparison::Equal;
                        }

                        1.0 - Self::calculate_predicate_selectivity(
                            self,
                            &not_predicate,
                            base_relation_scalar_index,
                            scalar_index,
                        )
                    }
                    Comparison::Greater => {
                        if let Scalar::ScalarLiteral(Literal::Int32(i)) = &**right {
                            let min = get_literal_int32(&min.unwrap());
                            let max = get_literal_int32(&max.unwrap());
                            (max - *i) as f64 / (max - min + 1) as f64
                        } else {
                            unimplemented!()
                        }
                    }
                    _ => unimplemented!(),
                }
            }
            Predicate::Negation { operand } => {
                1.0 - Self::calculate_predicate_selectivity(
                    self,
                    &**operand,
                    base_relation_scalar_index,
                    scalar_index,
                )
            }
            Predicate::Disjunction {
                static_operand_list,
                dynamic_operand_list,
            } => {
                debug_assert!(static_operand_list.is_empty());
                debug_assert!(!dynamic_operand_list.is_empty());

                let mut count = HashMap::new();
                for predicate in dynamic_operand_list {
                    Self::count_predicate_cardinality(self, predicate, &mut count);
                }

                let mut selectivity = 1.0;
                for (_, (values, cardinality)) in count {
                    selectivity *= values.len() as f64 / cardinality as f64;
                }
                selectivity
            }
        }
    }

    fn count_predicate_cardinality(
        &self,
        predicate: &Predicate,
        count: &mut HashMap<Scalar, (HashSet<i32>, usize)>,
    ) {
        match predicate {
            Predicate::Comparison {
                comparison,
                left,
                right,
            } => match comparison {
                Comparison::Equal => {
                    if let Scalar::ScalarAttribute(c) = &**left {
                        let stat = self.stat_info.get(c.name.as_str());
                        debug_assert!(stat.is_some());
                        let stat = stat.unwrap();
                        if let Some(transformer) = &stat.3 {
                            match transformer {
                                Transformer::StringLastChar => {
                                    if let Scalar::ScalarLiteral(Literal::Char(str)) = &**right {
                                        let value = str[str.len() - 1..].parse().unwrap();
                                        if let Some((values, _)) = count.get_mut(&**left) {
                                            values.insert(value);
                                        } else {
                                            let mut values = HashSet::new();
                                            values.insert(value);
                                            count.insert(left.as_ref().clone(), (values, stat.2));
                                        }
                                    } else {
                                        unimplemented!()
                                    }
                                }
                                _ => unimplemented!(),
                            }
                        }
                    }
                }
                _ => unimplemented!(),
            },
            _ => unimplemented!(),
        }
    }

    fn apply_to_star_join_aggr(
        &self,
        fact_table_filter: &Option<Predicate>,
        dim_tables: &Vec<JoinContext>,
        groups: &mut Vec<DataSource>,
        evaluation_order: &mut Vec<EvaluationOrder>,
        groups_from_fact_table: &mut Vec<usize>,
    ) {
        let mut base_relation_scalar_index = HashMap::new();
        let mut group_by_join_index = HashMap::new();
        for i in 0..groups.len() {
            match &groups[i] {
                DataSource::BaseRelation(scalar) => {
                    base_relation_scalar_index.insert(scalar.clone(), i);
                }
                DataSource::JoinHashTable(j) => {
                    group_by_join_index.insert(*j, i);
                }
            }
        }

        let mut groups_from_base_relation = vec![true; groups.len()];
        let mut predicates = BTreeMap::new();
        if let Some(predicate) = fact_table_filter {
            let mut scalar_index = -1;
            let selectivity = Self::calculate_predicate_selectivity(
                self,
                predicate,
                &base_relation_scalar_index,
                &mut Some(&mut scalar_index),
            );
            let referenced_attributes_from_predicate = get_referenced_attributes(predicate);
            debug_assert_eq!(1, referenced_attributes_from_predicate.len());
            for attribute in referenced_attributes_from_predicate {
                let table = attribute.table.as_str();
                if scalar_index == -1 {
                    predicates.insert(
                        (NonNan(selectivity), get_relation_size(table)),
                        EvaluationOrder::BaseRelation,
                    );
                } else {
                    let scalar_index = scalar_index as usize;
                    debug_assert!(scalar_index < groups.len());

                    predicates.insert(
                        (NonNan(selectivity), get_relation_size(table)),
                        EvaluationOrder::GroupByIndex(scalar_index),
                    );
                    groups_from_base_relation[scalar_index] = false;
                }
            }
        }

        for i in 0..dim_tables.len() {
            if let Some(predicate) = &dim_tables[i].predicate {
                let order = if let Some(j) = group_by_join_index.get(&i) {
                    let j = *j;
                    groups_from_base_relation[j] = false;
                    EvaluationOrder::GroupByIndex(j)
                } else {
                    EvaluationOrder::ExistenceJoin(i)
                };

                let referenced_attributes_from_predicate = get_referenced_attributes(predicate);
                debug_assert_eq!(1, referenced_attributes_from_predicate.len());
                for attribute in referenced_attributes_from_predicate {
                    let table = attribute.table.as_str();
                    predicates.insert(
                        (
                            NonNan(Self::calculate_predicate_selectivity(
                                self,
                                predicate,
                                &base_relation_scalar_index,
                                &mut None,
                            )),
                            get_relation_size(table),
                        ),
                        order.clone(),
                    );
                }
            }
        }

        evaluation_order.reserve(predicates.len());
        for ((NonNan(_selectivilty), _table_size), order) in predicates {
            evaluation_order.push(order);
        }

        for i in 0..groups_from_base_relation.len() {
            if groups_from_base_relation[i] {
                groups_from_fact_table.push(i);
            }
        }
    }
}

impl crate::rules::Rule for SetEvaluationOrder {
    fn apply(&self, _database: &Database, physical_plan: &mut PhysicalPlan) {
        if let PhysicalPlan::TopLevelPlan { plan, .. } = physical_plan {
            match &mut **plan {
                PhysicalPlan::Aggregate { .. } => {}
                PhysicalPlan::StarJoinAggregate {
                    fact_table: _,
                    fact_table_filter,
                    fact_table_join_column_ids: _,
                    dim_tables,
                    aggregate_context: _,
                    groups,
                    evaluation_order,
                    groups_from_fact_table,
                    output_schema: _,
                } => {
                    Self::apply_to_star_join_aggr(
                        self,
                        fact_table_filter,
                        dim_tables,
                        groups,
                        evaluation_order,
                        groups_from_fact_table,
                    );
                }
                PhysicalPlan::Sort { input, .. } => {
                    if let PhysicalPlan::StarJoinAggregate {
                        fact_table: _,
                        fact_table_filter,
                        fact_table_join_column_ids: _,
                        dim_tables,
                        aggregate_context: _,
                        groups,
                        evaluation_order,
                        groups_from_fact_table,
                        output_schema: _,
                    } = &mut **input
                    {
                        Self::apply_to_star_join_aggr(
                            self,
                            fact_table_filter,
                            dim_tables,
                            groups,
                            evaluation_order,
                            groups_from_fact_table,
                        );
                    }
                }
                _ => (),
            }
        }
    }

    fn name(&self) -> &'static str {
        "SetEvaluationOrder"
    }
}

fn get_literal_int32(literal: &Literal) -> i32 {
    if let Literal::Int32(i) = literal {
        *i
    } else {
        unimplemented!()
    }
}

fn parse_string_last_four_three(str: &String) -> i32 {
    let len = str.len();
    str[len - 4..len - 2].parse().unwrap()
}
fn parse_string_last_two_one(str: &String) -> i32 {
    str[str.len() - 2..].parse().unwrap()
}

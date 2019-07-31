use hustle_common::{
    Comparison, Database, Literal, PhysicalPlan, Predicate, Scalar, D_RELATION_NAME, SSB_DDATE,
};
use std::collections::HashMap;
use std::str::FromStr;

pub struct RewritePredicate {}

impl RewritePredicate {
    pub fn new() -> Self {
        RewritePredicate {}
    }
}

impl crate::rules::Rule for RewritePredicate {
    fn apply(&self, database: &Database, physical_plan: &mut PhysicalPlan) {
        if let PhysicalPlan::TopLevelPlan { plan, .. } = physical_plan {
            match &mut **plan {
                PhysicalPlan::Aggregate {
                    table,
                    aggregates: _,
                    groups: _,
                    filter,
                    ..
                } => {
                    if let Some(predicate) = filter {
                        rewrite_predicate(database, predicate);
                    }

                    apply_to_node(database, table);
                }
                PhysicalPlan::StarJoinAggregate { .. } => unreachable!(),
                PhysicalPlan::Sort { input, .. } => {
                    apply_to_node(database, input);
                }
                _ => unimplemented!(),
            }
        }
    }

    fn name(&self) -> &'static str {
        "RewritePredicate"
    }
}

fn apply_to_node(database: &Database, plan: &mut Box<PhysicalPlan>) {
    match &mut **plan {
        PhysicalPlan::Aggregate {
            table,
            aggregates: _,
            groups: _,
            filter,
            ..
        } => {
            if let Some(predicate) = filter {
                rewrite_predicate(database, predicate);
            }

            apply_to_node(database, table);
        }
        PhysicalPlan::StarJoin {
            fact_table: _,
            fact_table_filter,
            fact_table_join_column_ids: _,
            dim_tables,
            ..
        } => {
            if let Some(predicate) = fact_table_filter {
                rewrite_predicate(database, predicate);
            }

            for dim_table in dim_tables {
                if let Some(predicate) = &mut dim_table.predicate {
                    rewrite_predicate(database, predicate);
                }
            }
        }
        PhysicalPlan::TopLevelPlan { .. }
        | PhysicalPlan::Sort { .. }
        | PhysicalPlan::StarJoinAggregate { .. } => unreachable!(),
        _ => unimplemented!(),
    }
}

fn rewrite_predicate(database: &Database, predicate: &mut Predicate) {
    match predicate {
        Predicate::True | Predicate::False => (),
        Predicate::Between { operand: _, .. } => (),
        Predicate::Negation { .. } => unimplemented!(),
        Predicate::Comparison {
            comparison,
            left,
            right,
        } => match comparison {
            Comparison::Equal => {
                let lhs;
                if let Scalar::ScalarAttribute(c) = &**left {
                    lhs = c;
                } else {
                    return;
                }

                if lhs.name == "d_yearmonth" {
                    if let Scalar::ScalarLiteral(Literal::Char(s)) = &**right {
                        let d = database.find_table(&lhs.table).unwrap();
                        let d_yearmonthnum = d
                            .get_column_by_id(SSB_DDATE::D_YEARMONTHNUM as usize)
                            .unwrap();
                        *left = Box::new(Scalar::ScalarAttribute(d_yearmonthnum.clone()));
                        *right =
                            Box::new(Scalar::ScalarLiteral(Literal::Int32(parse_yearmonth(s))));
                    } else {
                        unreachable!()
                    }
                }
            }
            _ => unimplemented!(),
        },
        Predicate::Conjunction {
            static_operand_list,
            dynamic_operand_list,
        } => {
            debug_assert!(static_operand_list.is_empty());
            debug_assert!(!dynamic_operand_list.is_empty());

            let mut predicates = HashMap::new();

            // FIXME: Get this stat info from Column.
            let d_year_min_value = 1992;
            let d_year_max_value = 1998;
            for predicate in dynamic_operand_list {
                if let Predicate::Comparison {
                    comparison,
                    left,
                    right,
                } = predicate
                {
                    let mut entry;
                    if let Scalar::ScalarAttribute(c) = &**left {
                        let range = if c.name == "d_year" {
                            (d_year_min_value, d_year_max_value)
                        } else if c.name == "d_weeknuminyear" {
                            (1, 53)
                        } else {
                            return;
                        };
                        entry = predicates.entry(c.clone()).or_insert(range);
                    } else {
                        return;
                    }

                    let rhs;
                    if let Scalar::ScalarLiteral(Literal::Int32(i)) = &**right {
                        rhs = *i;
                    } else {
                        return;
                    }

                    match comparison {
                        Comparison::LessEqual => {
                            if rhs < entry.1 {
                                entry.1 = rhs;
                            }
                        }
                        Comparison::GreaterEqual => {
                            if entry.0 < rhs {
                                entry.0 = rhs;
                            }
                        }
                        Comparison::Equal => {
                            *entry = (rhs, rhs);
                        }
                        _ => unimplemented!(),
                    }
                }
            }

            if predicates.len() == 1 {
                for (column, (lo, hi)) in predicates {
                    if column.name == "d_year" {
                        if lo == d_year_min_value && hi == d_year_max_value {
                            *predicate = Predicate::True;
                        } else if lo == d_year_min_value {
                            let comparison = if hi + 1 == d_year_max_value {
                                Comparison::NotEqual
                            } else {
                                Comparison::Less
                            };
                            *predicate = Predicate::Comparison {
                                comparison,
                                left: Box::new(Scalar::ScalarAttribute(column)),
                                right: Box::new(Scalar::ScalarLiteral(Literal::Int32(hi + 1))),
                            };
                        } else if hi == d_year_max_value {
                            let comparison = if lo - 1 == d_year_min_value {
                                Comparison::NotEqual
                            } else {
                                Comparison::Less
                            };
                            *predicate = Predicate::Comparison {
                                comparison,
                                left: Box::new(Scalar::ScalarAttribute(column)),
                                right: Box::new(Scalar::ScalarLiteral(Literal::Int32(lo - 1))),
                            };
                        } else {
                            *predicate = Predicate::Between {
                                operand: Box::new(Scalar::ScalarAttribute(column)),
                                begin: Box::new(Scalar::ScalarLiteral(Literal::Int32(lo))),
                                end: Box::new(Scalar::ScalarLiteral(Literal::Int32(hi))),
                            };
                        }
                    }
                }
            } else {
                let d = database.find_table(D_RELATION_NAME).unwrap();
                let d_year = d.get_column_by_id(SSB_DDATE::D_YEAR as usize).unwrap();
                let d_weeknuminyear = d
                    .get_column_by_id(SSB_DDATE::D_WEEKNUMINYEAR as usize)
                    .unwrap();

                if predicates.contains_key(d_year) && predicates.contains_key(d_weeknuminyear) {
                    let d_year_range = predicates.get(d_year).unwrap();
                    debug_assert_eq!(d_year_range.0, d_year_range.1);
                    let year = d_year_range.0 * 10000;
                    let d_weeknuminyear_range = predicates.get(d_weeknuminyear).unwrap();
                    debug_assert_eq!(d_weeknuminyear_range.0, d_weeknuminyear_range.1);
                    // FIXME
                    debug_assert_eq!(6i32, d_weeknuminyear_range.0);

                    let d_datekey = d.get_column_by_id(SSB_DDATE::D_DATEKEY as usize).unwrap();
                    let operand = Box::new(Scalar::ScalarAttribute(d_datekey.clone()));
                    let begin = Box::new(Scalar::ScalarLiteral(Literal::Int32(204 + year))); // FIXME
                    let end = Box::new(Scalar::ScalarLiteral(Literal::Int32(210 + year))); // FIXME
                    *predicate = Predicate::Between {
                        operand,
                        begin,
                        end,
                    };
                }
            }
        }
        Predicate::Disjunction {
            static_operand_list,
            dynamic_operand_list,
        } => {
            debug_assert!(static_operand_list.is_empty());
            debug_assert!(!dynamic_operand_list.is_empty());

            // FIXME: Get this stat info from Column.
            let mut rhs = vec![false; 7];
            let min_value = 1992;
            for predicate in dynamic_operand_list {
                if let Predicate::Comparison {
                    comparison,
                    left,
                    right,
                } = predicate
                {
                    if let Comparison::Equal = comparison {
                        if let Scalar::ScalarAttribute(c) = &**left {
                            if c.name != "d_year" {
                                return;
                            }
                        } else {
                            return;
                        }

                        if let Scalar::ScalarLiteral(Literal::Int32(i)) = &**right {
                            rhs[(*i - min_value) as usize] = true;
                        } else {
                            return;
                        }
                    } else {
                        return;
                    }
                } else {
                    return;
                }
            }

            let mut lo = -1i32;
            while !rhs[(lo + 1) as usize] {
                lo += 1;
            }

            let mut hi = rhs.len() as i32;
            while !rhs[(hi - 1) as usize] {
                hi -= 1;
            }

            for i in lo + 1..hi - 1 {
                if !rhs[i as usize] {
                    return;
                }
            }

            let d = database.find_table(D_RELATION_NAME).unwrap();
            let d_year = d.get_column_by_id(SSB_DDATE::D_YEAR as usize).unwrap();
            if lo == -1 && hi == rhs.len() as i32 {
                *predicate = Predicate::True;
            } else if lo == -1 {
                *predicate = Predicate::Comparison {
                    comparison: Comparison::Less,
                    left: Box::new(Scalar::ScalarAttribute(d_year.clone())),
                    right: Box::new(Scalar::ScalarLiteral(Literal::Int32(hi + min_value))),
                };
            } else if hi == rhs.len() as i32 {
                *predicate = Predicate::Comparison {
                    comparison: Comparison::Greater,
                    left: Box::new(Scalar::ScalarAttribute(d_year.clone())),
                    right: Box::new(Scalar::ScalarLiteral(Literal::Int32(lo + min_value))),
                };
            } else {
                *predicate = Predicate::Between {
                    operand: Box::new(Scalar::ScalarAttribute(d_year.clone())),
                    begin: Box::new(Scalar::ScalarLiteral(Literal::Int32(lo + min_value + 1))),
                    end: Box::new(Scalar::ScalarLiteral(Literal::Int32(hi + min_value - 1))),
                };
            }
        }
    }
}

fn parse_yearmonth(yearmonth: &String) -> i32 {
    debug_assert_eq!(7, yearmonth.len());
    let year = i32::from_str(&yearmonth[3..]).unwrap();
    let month_str = &yearmonth[0..3];
    let month = if month_str == "Jan" {
        1
    } else if month_str == "Feb" {
        2
    } else if month_str == "Mar" {
        3
    } else if month_str == "Apr" {
        4
    } else if month_str == "May" {
        5
    } else if month_str == "Jun" {
        6
    } else if month_str == "Jul" {
        7
    } else if month_str == "Aug" {
        8
    } else if month_str == "Sep" {
        9
    } else if month_str == "Oct" {
        10
    } else if month_str == "Nov" {
        11
    } else if month_str == "Dec" {
        12
    } else {
        unreachable!()
    };

    year * 100 + month
}

use hustle_common::{
    get_referenced_attributes, predicate_replaces_new_scalar, replaces_new_scalar, BinaryOperation,
    Column, ColumnAnnotation, Database, Literal, PhysicalPlan, Predicate, Scalar,
};
use std::collections::{HashMap, HashSet};

pub struct EliminateJoin {}

impl EliminateJoin {
    pub fn new() -> Self {
        EliminateJoin {}
    }
}

impl crate::rules::Rule for EliminateJoin {
    fn apply(&self, database: &Database, physical_plan: &mut PhysicalPlan) {
        if let PhysicalPlan::TopLevelPlan { plan, .. } = physical_plan {
            match &mut **plan {
                PhysicalPlan::Aggregate { .. } => apply_to_node(database, plan),
                PhysicalPlan::Sort { input, .. } => apply_to_node(database, input),
                _ => (),
            }
        }
    }

    fn name(&self) -> &'static str {
        "JoinElimination"
    }
}

fn apply_to_node(database: &Database, plan: &mut Box<PhysicalPlan>) {
    if let PhysicalPlan::Aggregate {
        table,
        aggregates: _,
        groups,
        filter,
        ..
    } = &mut **plan
    {
        let mut need_eliminate = false;
        let mut new_input = table.clone();
        let mut new_filter = None;
        if let PhysicalPlan::StarJoin {
            fact_table,
            fact_table_filter,
            fact_table_join_column_ids,
            dim_tables,
            output_schema,
        } = &mut **table
        {
            debug_assert_eq!(fact_table_join_column_ids.len(), dim_tables.len());
            let fact_table_reference;
            if let PhysicalPlan::TableReference { table, .. } = &**fact_table {
                fact_table_reference = table;
            } else {
                return;
            }

            let mut dim_table_name_to_index = HashMap::new();
            for i in 0..dim_tables.len() {
                let dim_table = &dim_tables[i];
                dim_table_name_to_index.insert(dim_table.table_name.clone(), (i, vec![]));
            }

            // No output column from the dim table
            for output_column in output_schema {
                if let Scalar::ScalarAttribute(column) = output_column {
                    if &column.table != &fact_table_reference.name {
                        match column.annotation {
                            None => {
                                dim_table_name_to_index.remove(&column.table);
                            }
                            Some(ColumnAnnotation::DerivedFromPrimaryKey(_)) => {
                                dim_table_name_to_index
                                    .get_mut(&column.table)
                                    .unwrap()
                                    .1
                                    .push(column.clone());
                            }
                            _ => unimplemented!(),
                        }
                    }
                }
            }

            let mut additional_fact_table_filter = Vec::new();
            for (i, referenced_output_columns) in dim_table_name_to_index.values() {
                let i = *i;
                let fact_table_join_column =
                    &fact_table_reference.columns[fact_table_join_column_ids[i]];
                debug_assert_eq!(
                    Some(ColumnAnnotation::ForeignKey),
                    fact_table_join_column.annotation
                );
                let dim_table = &dim_tables[i];
                let dim_table_reference = database.find_table(&dim_table.table_name).unwrap();
                debug_assert_eq!(
                    Some(ColumnAnnotation::PrimaryKey),
                    dim_table_reference.columns[dim_table.join_column_id].annotation
                );

                let mut referenced_attributes: HashSet<Column> =
                    referenced_output_columns.into_iter().cloned().collect();
                if let Some(predicate) = &dim_table.predicate {
                    let referenced_attributes_from_predicate = get_referenced_attributes(predicate);
                    for referenced_attribute in referenced_attributes_from_predicate {
                        referenced_attributes.insert(referenced_attribute);
                    }
                }
                let referenced_attributes: Vec<Column> =
                    referenced_attributes.iter().cloned().collect();
                let mut referenced_attributes_convert =
                    Vec::with_capacity(referenced_attributes.len());
                for attribute in &referenced_attributes {
                    match attribute.annotation {
                        Some(ColumnAnnotation::PrimaryKey) => referenced_attributes_convert.push(
                            Box::new(Scalar::ScalarAttribute(fact_table_join_column.clone())),
                        ),
                        Some(ColumnAnnotation::ForeignKey) => unreachable!(),
                        Some(ColumnAnnotation::DerivedFromPrimaryKey(v)) => {
                            let left =
                                Box::new(Scalar::ScalarAttribute(fact_table_join_column.clone()));
                            let right = Box::new(Scalar::ScalarLiteral(Literal::Int32(v)));
                            referenced_attributes_convert.push(Box::new(
                                Scalar::BinaryExpression {
                                    operation: BinaryOperation::Divide,
                                    left,
                                    right,
                                },
                            ));
                        }
                        None => break, // Cannot eliminate this join, as no known method to covert
                    }
                }

                if referenced_attributes_convert.len() != referenced_attributes.len() {
                    // We encountered some column that cannot be convert
                    continue;
                }

                if let Some(predicate) = &dim_table.predicate {
                    let mut predicate_to_change = predicate.clone();
                    predicate_replaces_new_scalar(
                        &mut predicate_to_change,
                        &referenced_attributes,
                        &referenced_attributes_convert,
                    );
                    additional_fact_table_filter.push(predicate_to_change);
                }

                // FIXME: replace scalar in aggregates

                for i in 0..groups.len() {
                    replaces_new_scalar(
                        &mut groups[i],
                        &referenced_attributes,
                        &referenced_attributes_convert,
                    );
                }

                dim_tables.remove(i);
                fact_table_join_column_ids.remove(i);
            }

            if dim_tables.is_empty() {
                // All join eliminated.
                need_eliminate = true;
                new_input = fact_table.clone();

                new_filter = fact_table_filter.clone();
            }

            let additional_fact_table_filter_len = additional_fact_table_filter.len();
            if additional_fact_table_filter_len > 0 {
                if let Some(predicate) = &mut new_filter {
                    if let Predicate::Conjunction {
                        static_operand_list: _,
                        dynamic_operand_list,
                    } = predicate
                    {
                        // Assume additional_fact_table_filter has the best selectivity
                        additional_fact_table_filter.append(dynamic_operand_list);
                        *dynamic_operand_list = additional_fact_table_filter;
                    } else {
                        unimplemented!()
                    }
                } else if additional_fact_table_filter_len == 1 {
                    new_filter = Some(additional_fact_table_filter[0].clone());
                } else {
                    new_filter = Some(Predicate::Conjunction {
                        static_operand_list: vec![],
                        dynamic_operand_list: additional_fact_table_filter,
                    });
                }
            }
        }

        if need_eliminate {
            table.clone_from(&new_input);
            *filter = new_filter;
        } else if new_filter.is_some() {
            *filter = new_filter;
        }
    }
}

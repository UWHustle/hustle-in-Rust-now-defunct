use hustle_common::{
    get_referenced_attribute, merge_conjunction_predicates, DataSource, Database, PhysicalPlan,
};
use std::collections::HashMap;

pub struct FuseOperators {}

impl FuseOperators {
    pub fn new() -> Self {
        FuseOperators {}
    }
}

impl crate::rules::Rule for FuseOperators {
    fn apply(&self, _database: &Database, physical_plan: &mut PhysicalPlan) {
        if let PhysicalPlan::TopLevelPlan { plan, .. } = physical_plan {
            match &mut **plan {
                PhysicalPlan::Aggregate { .. } => apply_to_node(plan),
                PhysicalPlan::Sort { input, .. } => apply_to_node(input),
                _ => (),
            }
        }
    }

    fn name(&self) -> &'static str {
        "OperatorsFusion"
    }
}

fn apply_to_node(plan: &mut Box<PhysicalPlan>) {
    if let PhysicalPlan::Aggregate {
        table,
        aggregates,
        groups,
        filter,
        output_schema,
    } = &mut **plan
    {
        if let PhysicalPlan::StarJoin {
            fact_table,
            fact_table_filter,
            fact_table_join_column_ids,
            dim_tables,
            ..
        } = &mut **table
        {
            let fact_table_name;
            if let PhysicalPlan::TableReference { table, .. } = &**fact_table {
                fact_table_name = &table.name;
            } else {
                return;
            }

            let mut dim_table_name_to_index = HashMap::new();
            for i in 0..dim_tables.len() {
                let dim_table = &dim_tables[i];
                dim_table_name_to_index.insert(dim_table.table_name.clone(), i);
            }

            let mut group_by_sources = Vec::with_capacity(groups.len());
            for group in groups {
                let referenced_attribute = get_referenced_attribute(group);
                debug_assert_eq!(1usize, referenced_attribute.len());
                for attribute in referenced_attribute {
                    let referenced_table = &attribute.table;

                    let group_by_source = if referenced_table == fact_table_name {
                        DataSource::BaseRelation(group.clone())
                    } else {
                        DataSource::JoinHashTable(
                            *dim_table_name_to_index.get(referenced_table).unwrap(),
                        )
                    };

                    group_by_sources.push(group_by_source);
                }
            }

            let new_filter = if filter.is_some() && fact_table_filter.is_some() {
                Some(merge_conjunction_predicates(vec![
                    fact_table_filter.as_ref().unwrap(),
                    filter.as_ref().unwrap(),
                ]))
            } else if fact_table_filter.is_some() {
                fact_table_filter.clone()
            } else if filter.is_some() {
                filter.clone()
            } else {
                None
            };

            *plan = Box::new(PhysicalPlan::StarJoinAggregate {
                fact_table: fact_table.clone(),
                fact_table_filter: new_filter,
                fact_table_join_column_ids: fact_table_join_column_ids.clone(),
                dim_tables: dim_tables.clone(),
                aggregate_context: aggregates[0].clone(),
                groups: group_by_sources,
                evaluation_order: vec![],
                groups_from_fact_table: vec![],
                output_schema: output_schema.clone(),
            });
        }
    }
}

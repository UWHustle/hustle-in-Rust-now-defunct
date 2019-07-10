use logical_entities::aggregations::avg::Avg;
use logical_entities::aggregations::count::Count;
use logical_entities::aggregations::max::Max;
use logical_entities::aggregations::min::Min;
use logical_entities::aggregations::sum::Sum;
use logical_entities::aggregations::*;
use logical_entities::column::Column;
use logical_entities::relation::Relation;
use logical_entities::schema::Schema;
use physical_operators::Operator;
use hustle_types::borrowed_buffer::BorrowedBuffer;
use hustle_types::*;

use std::collections::HashMap;

use super::hustle_storage::StorageManager;

pub struct Aggregate {
    input_relation: Relation,
    output_relation: Relation,
    agg_col_in: Column,
    agg_col_out: Column,
    aggregation: Box<AggregationTrait>,
}

impl Aggregate {
    pub fn new(
        input_relation: Relation,
        agg_col_in: Column,
        agg_col_out: Column,
        output_col_names: Vec<String>,
        aggregation: Box<AggregationTrait>,
    ) -> Self {
        let mut output_cols: Vec<Column> = vec![];
        for col_str in output_col_names {
            if col_str == agg_col_out.get_name() {
                output_cols.push(agg_col_out.clone());
            } else {
                for col in input_relation.get_columns() {
                    if col.get_name() == col_str {
                        output_cols.push(Column::new(&col_str, col.data_type()));
                        break;
                    }
                }
            }
        }
        let schema = Schema::new(output_cols);
        let output_relation = Relation::new(&format!("{}_agg", input_relation.get_name()), schema);
        Aggregate {
            input_relation,
            output_relation,
            agg_col_in,
            agg_col_out,
            aggregation,
        }
    }

    pub fn from_str(
        input_relation: Relation,
        agg_col_in: Column,
        agg_col_out: Column,
        output_col_names: Vec<String>,
        agg_name: &str,
    ) -> Result<Self, String> {
        let aggregation: Box<AggregationTrait> = match agg_name.to_lowercase().as_str() {
            "avg" => Box::new(Avg::new(agg_col_out.data_type())),
            "count" => Box::new(Count::new(agg_col_out.data_type())),
            "max" => Box::new(Max::new(agg_col_out.data_type())),
            "min" => Box::new(Min::new(agg_col_out.data_type())),
            "sum" => Box::new(Sum::new(agg_col_out.data_type())),
            _ => return Err(format!("Unknown aggregate function {}", agg_name)),
        };
        Ok(Self::new(
            input_relation,
            agg_col_in,
            agg_col_out,
            output_col_names,
            aggregation,
        ))
    }
}

impl Operator for Aggregate {
    fn get_target_relation(&self) -> Option<Relation> {
        Some(self.output_relation.clone())
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Option<Relation>, String> {
        let in_schema = self.input_relation.get_schema();
        let in_physical_relation = storage_manager
            .relational_engine()
            .get(self.input_relation.get_name())
            .unwrap();
        let out_schema = self.output_relation.get_schema();
        storage_manager.relational_engine().drop(self.output_relation.get_name());

        // Indices of the group by and aggregate columns in the input relation
        let mut group_cols_i = vec![];
        let mut agg_col_i = 0;
        for mut col in out_schema.get_columns() {
            if col == &self.agg_col_out {
                col = &self.agg_col_in;
            }
            let i = in_schema
                .get_columns()
                .iter()
                .position(|x| x == col)
                .unwrap();
            if col == &self.agg_col_in {
                agg_col_i = i;
            } else {
                group_cols_i.push(i);
            }
        }

        // A HashMap mapping group by values to aggregations for that grouping
        let mut group_by: HashMap<Vec<Vec<u8>>, Box<AggregationTrait>> = HashMap::new();

        for in_block in in_physical_relation.blocks() {
            for row_i in 0..in_block.get_n_rows() {
                // Determine whether we've seen the current combination of group by values
                let mut group_buffs = vec![];
                for col_i in &group_cols_i {
                    // Caution: this just checks the slice and doesn't have logic for null handling
                    group_buffs.push(in_block.get_row_col(row_i, *col_i).unwrap().to_vec());
                }
                if !group_by.contains_key(&group_buffs) {
                    let mut aggregation = self.aggregation.box_clone_aggregation();
                    aggregation.initialize();
                    group_by.insert(group_buffs.clone(), aggregation);
                }

                // Consider the current value of the aggregate column
                let data = in_block.get_row_col(row_i, agg_col_i).unwrap();
                let data_type = self.agg_col_in.data_type();
                let agg_value = BorrowedBuffer::new(data, data_type, false).marshall();
                group_by
                    .get_mut(&group_buffs)
                    .unwrap()
                    .consider_value(&*agg_value);
            }
        }

        // Write group by and aggregate values to the output schema
        let out_physical_relation = storage_manager
            .relational_engine()
            .create(self.output_relation.get_name(), out_schema.to_size_vec());

        for (group_buffs, aggregation) in group_by {
            let mut group_i = 0;
            let mut row_builder = out_physical_relation.insert_row();
            for col in out_schema.get_columns() {
                if col == &self.agg_col_out {
                    row_builder.push(aggregation.output().un_marshall().data());
                } else {
                    row_builder.push(&group_buffs[group_i]);
                    group_i += 1;
                }
            }
        }

        Ok(self.get_target_relation())
    }
}

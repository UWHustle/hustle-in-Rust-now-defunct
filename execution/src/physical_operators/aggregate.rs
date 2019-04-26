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
use type_system::borrowed_buffer::BorrowedBuffer;
use type_system::data_type::DataType;
use type_system::*;

use std::collections::HashMap;

use super::storage::StorageManager;

pub struct Aggregate {
    input_relation: Relation,
    output_relation: Relation,
    agg_col: Column,
    group_by_cols: Vec<Column>,
    aggregation: Box<AggregationTrait>,
}

impl Aggregate {
    pub fn new(
        input_relation: Relation,
        agg_col: Column,
        group_by_cols: Vec<Column>,
        aggregation: Box<AggregationTrait>,
    ) -> Self {
        let agg_col_name = format!("{}({})", aggregation.get_name(), agg_col.get_name());
        let mut output_cols = group_by_cols.clone();
        output_cols.push(Column::new(agg_col_name, aggregation.output_type()));

        let schema = Schema::new(output_cols);
        let output_relation = Relation::new(&format!("{}_agg", input_relation.get_name()), schema);

        Aggregate {
            input_relation,
            output_relation,
            agg_col,
            group_by_cols,
            aggregation,
        }
    }

    pub fn from_str(
        input_relation: Relation,
        agg_col: Column,
        group_by_cols: Vec<Column>,
        agg_type: DataType,
        agg_name: &str,
    ) -> Result<Self, String> {
        let lower = agg_name.to_lowercase();
        let aggregation: Box<AggregationTrait> = match lower.as_str() {
            "avg" => Box::new(Avg::new(agg_type)),
            "count" => Box::new(Count::new(agg_type)),
            "max" => Box::new(Max::new(agg_type)),
            "min" => Box::new(Min::new(agg_type)),
            "sum" => Box::new(Sum::new(agg_type)),
            _ => {
                return Err(String::from(format!(
                    "Unknown aggregate function {}",
                    agg_name
                )));
            }
        };
        Ok(Self::new(
            input_relation,
            agg_col,
            group_by_cols,
            aggregation,
        ))
    }
}

impl Operator for Aggregate {
    fn get_target_relation(&self) -> Relation {
        self.output_relation.clone()
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Relation, String> {
        let in_schema = self.input_relation.get_schema();
        let in_record = storage_manager
            .get_with_schema(self.input_relation.get_name(), &in_schema.to_size_vec())
            .unwrap();
        let out_schema = self.output_relation.get_schema();

        // Indices of the group by and aggregate columns in the input relation
        let mut group_cols_i = vec![];
        let mut agg_col_i = 0;
        for col in out_schema.get_columns() {
            let i = in_schema
                .get_columns()
                .iter()
                .position(|&x| &x == col)
                .unwrap();
            if col == &self.agg_col {
                agg_col_i = i;
            } else {
                group_cols_i.push(i);
            }
        }

        // A HashMap mapping group by values to aggregations for that grouping
        let mut group_by: HashMap<Vec<&[u8]>, Box<AggregationTrait>> = HashMap::new();

        for in_block in in_record.blocks() {
            for row_i in 0..in_block.len() {
                // Determine whether we've seen the current combination of group by values
                let mut group_buffs = vec![];
                for col_i in group_cols_i {
                    // Caution: this just checks the slice and doesn't have logic for null handling
                    group_buffs.push(in_block.get_row_col(row_i, col_i).unwrap());
                }
                if !group_by.contains_key(&group_buffs) {
                    let mut aggregation = self.aggregation.box_clone_aggregation();
                    aggregation.initialize();
                    group_by.insert(group_buffs, aggregation);
                }

                // Consider the current value of the aggregate column
                let data = in_block.get_row_col(row_i, agg_col_i).unwrap();
                let data_type = self.agg_col.data_type();
                let agg_value = BorrowedBuffer::new(data, data_type, false).marshall();
                group_by[&group_buffs].consider_value(&*agg_value);
            }
        }

        // Write group by and aggregate values to the output schema
        for (group_buffs, aggregation) in group_by {
            let group_i = 0;
            for col in out_schema.get_columns() {
                if col == &self.agg_col {
                    storage_manager.append(
                        self.output_relation.get_name(),
                        self.aggregation.output().un_marshall().data(),
                    );
                } else {
                    storage_manager
                        .append(self.output_relation.get_name(), group_buffs[group_i]);
                    group_i += 1;
                }
            }
        }

        Ok(self.get_target_relation())
    }
}

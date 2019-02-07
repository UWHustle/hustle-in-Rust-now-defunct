use logical_entities::aggregations::AggregationTrait;
use logical_entities::column::Column;
use logical_entities::relation::Relation;
use logical_entities::schema::Schema;
use physical_operators::Operator;
use storage_manager::StorageManager;
use logical_entities::types::*;
use logical_entities::types::borrowed_buffer::BorrowedBuffer;

use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug)]
pub struct Aggregate<T: AggregationTrait> {
    input_relation: Relation,
    output_relation: Relation,
    group_by_cols: Vec<Column>,
    aggregation: T,
}

impl<T: AggregationTrait> Aggregate<T> {
    pub fn new(input_relation: Relation, agg_col: Column, group_by_cols: Vec<Column>, aggregation: T) -> Self {
        let agg_col_name = format!("{}({})", aggregation.get_name(), agg_col.get_name());
        let mut output_cols = group_by_cols.clone();
        output_cols.push(Column::new(agg_col_name, aggregation.output_type()));

        let schema = Schema::new(output_cols);
        let output_relation = Relation::new(format!("{}_agg", input_relation.get_name()), schema);

        Aggregate {
            input_relation,
            output_relation,
            group_by_cols,
            aggregation,
        }
    }
}

impl<T: AggregationTrait + Clone + Debug> Operator for Aggregate<T> {
    fn get_target_relation(&self) -> Relation {
        self.output_relation.clone()
    }

    fn execute(&self) -> Relation {
        let input_cols = self.input_relation.get_columns().to_vec();
        let input_data = StorageManager::get_full_data(&self.input_relation.clone());
        let mut output_data = StorageManager::create_relation(&self.output_relation, self.input_relation.get_total_size());

        //A HashMap mapping group by values to aggregations for that grouping
        let mut group_by: HashMap<Vec<(&str, Column)>, T> = HashMap::new();

        let mut i = 0; // Current position in the input buffer
        let mut j = 0; // Current position in the output buffer

        // Loop over all the data
        while i < input_data.len() {
            let mut group_by_values: Vec<(&str, Column)> = vec!();
            let mut agg_values: Vec<(&str, Column)> = vec!();

            // Split data by the columns relevant to the aggregation vs columns to group by
            for column in &input_cols {
                let value_len = column.get_datatype().size(); // TODO: Doesn't work for variable-length data
                let buffer = BorrowedBuffer::new(column.get_datatype(), false, &input_data[i..i + value_len]);
                let value = (buffer.marshall().to_str(), column.clone());
                if (&self.group_by_cols).into_iter().any(|c| c.get_name() == column.get_name()) {
                    group_by_values.push(value);
                } else {
                    agg_values.push(value);
                }
                i += value_len;
            }

            // Populate the GroupBy HashMap
            if group_by.contains_key(&group_by_values) {
                let agg_instance = group_by.get_mut(&group_by_values).unwrap();
                for value in agg_values {
                    agg_instance.consider_value(&*value.1.get_datatype().parse(value.0));
                }
            } else {
                let mut agg_instance = self.aggregation.clone();
                agg_instance.initialize();
                for value in agg_values {
                    agg_instance.consider_value(&*value.1.get_datatype().parse(value.0));
                }
                group_by.entry(group_by_values).or_insert(agg_instance);
            }
        }

        // Output a relation
        for (group_by_values, agg_instance) in &group_by {

            // Copy group by values
            for (data, column) in group_by_values {
                let data = column.get_datatype().parse(data).un_marshall();
                output_data[j..j + data.data().len()].clone_from_slice(&data.data());
                j += data.data().len();
            }

            // Copy aggregated values
            let output: &ValueType = &*agg_instance.output();
            output_data[j..j + output.un_marshall().data().len()].clone_from_slice(&output.un_marshall().data());
            j += output.un_marshall().data().len();
        }

        StorageManager::flush(&output_data);
        StorageManager::trim_relation(&self.output_relation, j);
        self.get_target_relation()
    }
}
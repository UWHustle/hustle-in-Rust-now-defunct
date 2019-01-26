use logical_entities::relation::Relation;
use logical_entities::column::Column;

use logical_entities::aggregations::AggregationTrait;

use storage_manager::StorageManager;

use physical_operators::Operator;

use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug)]
pub struct Aggregate<T: AggregationTrait + Clone> {
    relation: Relation,
    output_relation: Relation,
    aggregation: T
}

impl<T: AggregationTrait + Clone> Aggregate<T> {
    pub fn new(aggregation: T) -> Self {
        let relation = aggregation.input_relation();
        let schema = aggregation.output_schema();
        let output_relation = Relation::new(format!("{}{}", relation.get_name(), "_agg".to_string()), schema);
        Aggregate {
            relation,
            output_relation,
            aggregation
        }
    }
}

impl<T: AggregationTrait + Clone + Debug> Operator for Aggregate<T> {
    fn get_target_relation(&self) -> Relation {
        self.output_relation.clone()
    }

    fn execute(&self) -> Relation{
        let mut output_data = StorageManager::create_relation(&self.output_relation, self.relation.get_total_size());

        let input_columns: Vec<Column> = self.relation.get_columns().to_vec();

        let input_data = StorageManager::get_full_data(&self.relation.clone());

        let grouping_columns = self.aggregation.group_by_columns().clone();


        //A HashMap mapping group by values to aggregations for that grouping
        let mut group_by: HashMap<Vec<(Vec<u8>,Column)>, T>  = HashMap::new();

        let mut i = 0;
        let mut j = 0;

        // Loop over all the data
        while i < input_data.len() {


            let mut group_by_values: Vec<(Vec<u8>, Column)> = vec!();
            let mut aggregate_values: Vec<(Vec<u8>, Column)> = vec!();

            // Split data by the columns relevant to the aggregation vs columns to group by
            for column in &input_columns {
                let value_length = column.get_datatype().get_next_length(&input_data[i..]);
                let value = (input_data[i..i + value_length].to_vec(), column.clone());

                if (&grouping_columns).into_iter().any(|c| c.get_name() == column.get_name()) {
                    group_by_values.push(value);
                } else {
                    aggregate_values.push(value);
                }

                i += value_length;
            }

            // Populate the GroupBy HashMap
            if group_by.contains_key(&group_by_values) {
                let aggregate_instance = group_by.get_mut(&group_by_values).unwrap();
                for value in aggregate_values {
                    aggregate_instance.consider_value(value.0, value.1.clone());
                }
            } else {
                let mut aggregate_instance = self.aggregation.clone();
                aggregate_instance.initialize();

                for value in aggregate_values {
                    aggregate_instance.consider_value(value.0, value.1.clone());
                }

                group_by.entry(group_by_values).or_insert(aggregate_instance);
            }

        }

        // Output a relation

        for (group_by_values, aggregate_instance) in &group_by {
            for (data, _column) in group_by_values {
                output_data[j..j + data.len()].clone_from_slice(&data);
                j += data.len();
            }

            let output:Vec<u8> = aggregate_instance.output();
            output_data[j..j + output.len()].clone_from_slice(&output);
            j += output.len();
        }

        StorageManager::flush(&output_data);


        StorageManager::trim_relation(&self.output_relation, j);



        self.get_target_relation()
    }
}
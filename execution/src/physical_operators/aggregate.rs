use logical_entities::relation::Relation;
use logical_entities::column::Column;
use logical_entities::schema::Schema;

use logical_entities::aggregations::AggregationTrait;

use storage_manager::StorageManager;

use physical_operators::Operator;

use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug)]
pub struct Aggregate<T: AggregationTrait + Clone> {
    input_relation: Relation,
    output_relation: Relation,
    grouping_columns: Vec<Column>,
    aggregation: T
}

impl<T: AggregationTrait + Clone> Aggregate<T> {
    pub fn new(input_relation: Relation, agg_column: Column, grouping_columns: Vec<Column>, aggregation: T) -> Self {

        let agg_column_out = Column::new(format!("{}({})", aggregation.get_name(), agg_column.get_name()), "Int".to_string());
        let mut my_columns = grouping_columns.clone();
        my_columns.push(agg_column_out);

        let schema = Schema::new(my_columns);

        let output_relation = Relation::new(format!("{}_agg", input_relation.get_name()), schema);
        Aggregate {
            input_relation,
            output_relation,
            grouping_columns,
            aggregation
        }
    }
}

impl<T: AggregationTrait + Clone + Debug> Operator for Aggregate<T> {
    fn get_target_relation(&self) -> Relation {
        self.output_relation.clone()
    }

    fn execute(&self) -> Relation{
        let mut output_data = StorageManager::create_relation(&self.output_relation, self.input_relation.get_total_size());

        let input_columns: Vec<Column> = self.input_relation.get_columns().to_vec();

        let input_data = StorageManager::get_full_data(&self.input_relation.clone());

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

                if (&self.grouping_columns).into_iter().any(|c| c.get_name() == column.get_name()) {
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
                    aggregate_instance.consider_value(value.0);
                }
            } else {
                let mut aggregate_instance = self.aggregation.clone();
                aggregate_instance.initialize();

                for value in aggregate_values {
                    aggregate_instance.consider_value(value.0);
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
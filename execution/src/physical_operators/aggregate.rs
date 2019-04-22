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
                )))
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
        let input_cols = self.input_relation.get_columns().to_vec();
        let input_data = storage_manager.get(self.input_relation.get_name()).unwrap();

        // Future optimization: create uninitialized Vec (this may require unsafe Rust)
        let output_size =
            self.output_relation.get_row_size() * self.input_relation.get_n_rows(storage_manager);
        let mut output_data: Vec<u8> = vec![0; output_size];

        //A HashMap mapping group by values to aggregations for that grouping
        let mut group_by: HashMap<Vec<(String, Column)>, Box<AggregationTrait>> = HashMap::new();

        let mut i = 0; // Current position in the input buffer
        let mut j = 0; // Current position in the output buffer

        // Loop over all the data
        while i < input_data.len() {
            let mut group_by_values: Vec<(String, Column)> = vec![];
            let mut agg_values: Vec<(String, Column)> = vec![];

            // Split data by the columns relevant to the aggregation vs columns to group by
            for column in &input_cols {
                let value_len = column.get_datatype().next_size(&input_data[i..]);
                let buffer = BorrowedBuffer::new(
                    &input_data[i..i + value_len],
                    column.get_datatype(),
                    false,
                );
                let value = (buffer.marshall().to_string(), column.clone());
                if (&self.group_by_cols)
                    .iter()
                    .any(|c| c.get_name() == column.get_name())
                {
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
                    agg_instance.consider_value(&*value.1.get_datatype().parse(&value.0)?);
                }
            } else {
                let mut agg_instance = self.aggregation.box_clone_aggregation();
                agg_instance.initialize();
                for value in agg_values {
                    agg_instance.consider_value(&*value.1.get_datatype().parse(&value.0)?);
                }
                group_by.entry(group_by_values).or_insert(agg_instance);
            }
        }

        // Output a relation
        for (group_by_values, agg_instance) in &group_by {
            // Copy group by values
            for (data, column) in group_by_values {
                let data = column.get_datatype().parse(data)?.un_marshall();
                output_data[j..j + data.data().len()].clone_from_slice(&data.data());
                j += data.data().len();
            }

            // Copy aggregated values
            let output: &Value = &*agg_instance.output();
            output_data[j..j + output.un_marshall().data().len()]
                .clone_from_slice(&output.un_marshall().data());
            j += output.un_marshall().data().len();
        }

        output_data.resize(j, 0);
        storage_manager.put(self.output_relation.get_name(), &output_data);

        Ok(self.get_target_relation())
    }
}

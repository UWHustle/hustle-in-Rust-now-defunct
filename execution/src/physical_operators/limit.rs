/*
Limit physical operator

The output is the first limit rows of the input relation.
*/
use logical_entities::column::Column;
use logical_entities::relation::Relation;
use logical_entities::schema::Schema;

use storage_manager::StorageManager;

use physical_operators::Operator;

#[derive(Debug)]
pub struct Limit {
    relation: Relation,
    limit: u32,
    output_relation: Relation,
}

impl Limit {
    pub fn new(relation: Relation, limit: u32) -> Limit {
        let schema = Schema::new(relation.get_columns().clone());
        let output_relation = Relation::new(
            format!("{}{}", relation.get_name(), "_limit".to_string()),
            schema,
        );
        Limit {
            relation,
            limit,
            output_relation,
        }
    }
}

impl Operator for Limit {
    fn get_target_relation(&self) -> Relation {
        self.output_relation.clone()
    }

    fn execute(&self) -> Relation {
        let mut output_data = StorageManager::create_relation(
            &self.output_relation,
            self.relation.get_row_size() * (self.limit as usize),
        );

        let columns: Vec<Column> = self.relation.get_columns().to_vec();
        let input_data = StorageManager::get_full_data(&self.relation);

        let mut i = 0;
        let mut records = 0;
        while i < input_data.len() && records < self.limit {
            for column in &columns {
                let value_length = column.get_datatype().next_size(&input_data[i..]);
                output_data[i..i + value_length].clone_from_slice(&input_data[i..i + value_length]);
                i += value_length;
            }
            records += 1;
        }

        StorageManager::trim_relation(&self.output_relation, i);
        self.get_target_relation()
    }
}

use logical_entities::relation::Relation;
use logical_entities::column::Column;
use logical_entities::schema::Schema;

use storage_manager::StorageManager;

use physical_operators::Operator;

pub const CHUNK_SIZE:usize = 1024*1024;

#[derive(Debug)]
pub struct Project {
    relation: Relation,
    output_relation: Relation,
}

impl Project {
    pub fn new(relation: Relation, output_columns:Vec<Column>) -> Project {
        let schema = Schema::new(output_columns);
        let output_relation = Relation::new(format!("{}{}", relation.get_name(), "_project".to_string()), schema);
        Project {
            relation,
            output_relation,
        }
    }
}

impl Operator for Project {
    fn get_target_relation(&self) -> Relation {
        self.output_relation.clone()
    }

    fn execute(&self) -> Relation{
        let mut output_data = StorageManager::create_relation(&self.output_relation, self.relation.get_total_size());

        let input_columns:Vec<Column> = self.relation.get_columns().to_vec();
        let output_columns:Vec<Column> = self.output_relation.get_columns().to_vec();

        let input_data = StorageManager::get_full_data(&self.relation.clone());

        let mut i = 0;
        let mut j = 0;
        while i < input_data.len() {
            for column in &input_columns {
                let value_length = column.get_datatype().get_next_length(&input_data[i..]);

                if (&output_columns).into_iter().any(|x| { x.get_name() == column.get_name()}) {
                    output_data[j..j + value_length].clone_from_slice(&input_data[i..i + value_length]);
                    j += value_length;
                }
                i += value_length;
            }
        }

        self.get_target_relation()
    }
}
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
    predicate_name: String,
    comparator: i8,
    comp_value: Vec<u8>,
}

impl Project {
    pub fn new(relation: Relation, output_columns:Vec<Column>, predicate_name: String, comparator: i8, comp_value: Vec<u8>) -> Project {
        let schema = Schema::new(output_columns);
        let output_relation = Relation::new(format!("{}{}", relation.get_name(), "_project".to_string()), schema);
        Project {
            relation,
            output_relation,
            predicate_name,
            comparator,
            comp_value,
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
        let mut k = 0;
        let mut j = 0;

        while i < input_data.len() {
            let mut filter: bool = true;
            if self.comparator >= (-1) && self.comparator <=1 {
                for column in &input_columns {
                    let value_length = column.get_datatype().get_next_length(&input_data[k..]);

                    if  column.get_name().to_string() == self.predicate_name {
                           //let mut s:Vec<u8> = Vec::new();
                           //let s = column.get_datatype().sum(&s, &input_data[i..i + value_length].to_vec()).0;
                           let value = &input_data[k..k + value_length].to_vec();
                           if !( column.get_datatype().compare(value, &self.comp_value) == self.comparator){
                                filter=false;

                           }
                           //if !value.std::cmp::Eq(self.comp_value){
                           //   filter = false;

                        //}
                    }
                    k += value_length;
                }
            }
            if filter {
            k=i;
            for column in &input_columns {
                let value_length = column.get_datatype().get_next_length(&input_data[k..]);

                if (&output_columns).into_iter().any(|x| { x.get_name() == column.get_name()}) {
                    output_data[j..j + value_length].clone_from_slice(&input_data[k..k + value_length]);
                    j += value_length;

                }
                k += value_length;

            }
          }
            i = k;
        }

        StorageManager::trim_relation(&self.output_relation, j);

        self.get_target_relation()
    }
}
extern crate rand;
extern crate csv;

use type_system::*;
use logical_entities::relation::Relation;

use storage_manager::StorageManager;

use physical_operators::Operator;

//#[derive(Debug)]
pub struct RandomRelation {
    relation: Relation,
    row_count: usize
}

impl RandomRelation {
    pub fn new(relation: Relation, row_count: usize) -> Self {
        RandomRelation {
            relation,
            row_count
        }
    }
}

impl Operator for RandomRelation{
    fn get_target_relation(&self) -> Relation {
        self.relation.clone()
    }

    fn execute(&self) -> Relation {
        #[warn(unused_variables)]
        let mut data = StorageManager::create_relation(&self.relation, (self.relation.get_row_size() * self.row_count) as usize);

        let columns = self.relation.get_columns();
        let mut n : usize = 0;

        for _y in 0..self.row_count {
            for column in columns.iter() {
                let random_value = rand::random::<u8>().to_string();
                let value = column.get_datatype().parse(&random_value);
                let size = value.size();
                data[n..n + size].clone_from_slice(&value.un_marshall().data());
                n = n + size;
            }
        }

        StorageManager::flush(&data);
        self.get_target_relation()
    }
}
extern crate rand;
extern crate csv;

use type_system::*;
use logical_entities::relation::Relation;

use storage_manager::StorageManager;

use physical_operators::Operator;

//#[derive(Debug)]
pub struct RandomRelation {
    relation: Relation,
    row_count: usize,
    random: bool
}

impl TestRelation {
    pub fn new(relation: Relation, row_count: usize, random:bool) -> Self {
        TestRelation {
            relation,
            row_count,
            random
        }
    }
}

impl Operator for TestRelation {
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
                let mut value;
                if self.random {
                    value = rand::random::<u8>().to_string();
                } else {
                    value = _y.to_string();
                }
                let parsed = column.get_datatype().parse(&value);
                let size = value.size();
                data[n..n + size].clone_from_slice(&parsed.un_marshall().data());
                n = n + size;
            }
        }

        StorageManager::flush(&data);
        self.get_target_relation()
    }
}
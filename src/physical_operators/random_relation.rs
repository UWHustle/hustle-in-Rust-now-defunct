extern crate rand;
extern crate csv;

use logical_entities::relation::Relation;

use storage_manager::StorageManager;

#[derive(Debug)]
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

    pub fn execute(&self) -> bool {
        #[warn(unused_variables)]
        let mut data = StorageManager::create_relation(&self.relation, (self.relation.get_row_size() * self.row_count) as usize);

        let columns = self.relation.get_columns();
        let mut n : usize = 0;

        for _y in 0..self.row_count {
            for column in columns.iter() {
                let random_value = rand::random::<u32>().to_string();

                let (c,size) = column.get_datatype().parse_and_marshall(random_value);
                data[n..n + size].clone_from_slice(&c);
                n = n + size;
            }
        }

        true
    }
}
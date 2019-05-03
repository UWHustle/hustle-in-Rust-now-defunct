extern crate csv;
extern crate rand;

use logical_entities::relation::Relation;
use physical_operators::Operator;
use type_system::*;

use super::storage::StorageManager;

pub struct TestRelation {
    relation: Relation,
    row_count: usize,
    random: bool,
}

impl TestRelation {
    pub fn new(relation: Relation, row_count: usize, random: bool) -> Self {
        TestRelation {
            relation,
            row_count,
            random,
        }
    }
}

impl Operator for TestRelation {
    fn get_target_relation(&self) -> Relation {
        self.relation.clone()
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Relation, String> {
        // Future optimization: create uninitialized Vec (this may require unsafe Rust)
        let size = self.relation.get_row_size() * self.row_count;
        let mut data = vec![0; size];

        let mut n: usize = 0;
        for y in 0..self.row_count {
            for column in self.relation.get_columns() {
                let mut value = if self.random {
                    rand::random::<u8>().to_string()
                } else {
                    y.to_string()
                };
                let parsed = column.data_type().parse(&value)?;
                let size = parsed.size();
                data[n..n + size].clone_from_slice(&parsed.un_marshall().data());
                n += size;
            }
        }

        storage_manager.put(self.relation.get_name(), &data);

        Ok(self.get_target_relation())
    }
}

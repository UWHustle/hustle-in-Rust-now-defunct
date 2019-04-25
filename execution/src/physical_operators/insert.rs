use logical_entities::relation::Relation;
use logical_entities::row::Row;
use physical_operators::Operator;
use type_system::*;

use super::storage::StorageManager;

pub struct Insert {
    relation: Relation,
    row: Row,
}

impl Insert {
    pub fn new(relation: Relation, row: Row) -> Self {
        Insert { relation, row }
    }
}

impl Operator for Insert {
    fn get_target_relation(&self) -> Relation {
        self.relation.clone()
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Relation, String> {
        let mut data: Vec<u8> = vec![];
        match storage_manager.get(self.relation.get_name()) {
            Some(value) => data = value.to_vec(),
            None => (),
        }

        let mut n = data.len();
        data.resize(n + self.row.get_size(), 0);
        for value in self.row.get_values() {
            let size = value.size();
            data[n..n + size].clone_from_slice(value.un_marshall().data());
            n += size;
        }

        storage_manager.put(self.relation.get_name(), &data);

        Ok(self.get_target_relation())
    }
}

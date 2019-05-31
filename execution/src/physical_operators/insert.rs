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
    fn get_target_relation(&self) -> Option<Relation> {
        None
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Option<Relation>, String> {
        let mut row_builder = storage_manager
            .relational_engine()
            .get(self.relation.get_name())
            .unwrap()
            .insert_row();

        for value in self.row.get_values() {
            row_builder.push(value.un_marshall().data());
        }

        Ok(self.get_target_relation())
    }
}

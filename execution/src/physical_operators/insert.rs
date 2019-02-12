use logical_entities::relation::Relation;
use logical_entities::row::Row;
use logical_entities::types::*;

use physical_operators::Operator;

use storage_manager::StorageManager;

pub struct Insert {
    relation: Relation,
    row: Row,
}

impl Insert {
    pub fn new(relation: Relation, row: Row) -> Self {
        Insert {
            relation,
            row
        }
    }
}

impl Operator for Insert {
    fn get_target_relation(&self) -> Relation {
        self.relation.clone()
    }

    fn execute(&self) -> Relation {
        let row_size = self.row.get_size();

        let mut data = StorageManager::append_relation(&self.relation, row_size);

        let mut n = 0;
        for (i, column) in self.row.get_schema().get_columns().iter().enumerate() {
            let a = format!("{}",self.row.get_values()[i].to_string());

            let value = column.get_datatype().parse(&a);
            let size = value.size();
            data[n..n + size].clone_from_slice(value.un_marshall().data()); // 0  8
            n = n + size;
        }

        StorageManager::flush(&data);
        self.get_target_relation()
    }
}
use logical_entities::relation::Relation;
use logical_entities::row::Row;

use physical_operators::Operator;

use storage_manager::StorageManager;

#[derive(Debug)]
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
            let a = format!("{}",self.row.get_values()[i]);

            let (marshalled_value, size) = column.get_datatype().parse_and_marshall(a);
            data[n..n + size].clone_from_slice(&marshalled_value); // 0  8
            n = n + size;
        }

        StorageManager::flush(&data);
        self.get_target_relation()
    }
}
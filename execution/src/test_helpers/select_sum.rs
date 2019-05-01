use logical_entities::column::Column;
use logical_entities::relation::Relation;
use type_system::borrowed_buffer::*;
use type_system::*;

use super::storage::StorageManager;

pub struct SelectSum {
    relation: Relation,
    column: Column,
}

impl SelectSum {
    pub fn new(relation: Relation, column: Column) -> Self {
        SelectSum { relation, column }
    }

    pub fn execute(&self, storage_manager: &StorageManager) -> String {
        let schema = self.relation.get_schema();
        let record = storage_manager
            .get_with_schema(self.relation.get_name(), &schema.to_size_vec())
            .unwrap();

        // Index of the specified column
        let mut col_i = schema
            .get_columns()
            .iter()
            .position(|&x| x == self.column)
            .unwrap();

        let mut sum = self.column.data_type().create_zero();
        for block in record.blocks() {
            for data in block.get_col(col_i).unwrap() {
                let data_type = self.column.data_type();
                let value = BorrowedBuffer::new(data, data_type, false).marshall();
                sum = sum.add(force_numeric(&*value));
            }
        }

        sum.to_string()
    }
}

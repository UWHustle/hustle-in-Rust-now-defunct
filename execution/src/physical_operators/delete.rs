use logical_entities::relation::Relation;
use logical_entities::predicates::Predicate;
use physical_operators::Operator;
use super::storage::StorageManager;
use type_system::borrowed_buffer::BorrowedBuffer;
use type_system::Buffer;
use logical_entities::row::Row;

pub struct Delete {
    relation: Relation,
    predicate: Option<Box<Predicate>>
}

impl Delete {
    pub fn new(relation: Relation, predicate: Option<Box<Predicate>>) -> Self {
        Delete {
            relation,
            predicate
        }
    }
}

impl Operator for Delete {
    fn get_target_relation(&self) -> Relation {
        self.relation.clone()
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Relation, String> {
        let schema = self.relation.get_schema();
        let physical_relation = storage_manager
            .relational_engine()
            .get(self.relation.get_name())
            .unwrap();

        if let Some(ref predicate) = self.predicate {
            for mut block in physical_relation.blocks() {
                let mut row_i = 0;
                while row_i < block.get_n_rows() {
                    // Assemble values in the current row (this is very inefficient!).
                    let mut values = vec![];
                    for col_i in 0..schema.get_columns().len() {
                        let data = block.get_row_col(row_i, col_i).unwrap();
                        let data_type = schema.get_columns()[col_i].data_type();
                        let buff = BorrowedBuffer::new(&data, data_type, false);
                        values.push(buff.marshall());
                    }

                    // Check whether the current row satisfies the predicate.
                    let row = Row::new(schema.clone(), values);
                    if predicate.evaluate(&row) {
                        block.delete_row(row_i);
                    } else {
                        row_i += 1;
                    }
                }
            }

        } else {
            physical_relation.clear();
        }

        Ok(self.get_target_relation())
    }
}

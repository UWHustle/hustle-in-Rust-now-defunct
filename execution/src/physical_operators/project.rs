use logical_entities::column::Column;
use logical_entities::predicates::tautology::*;
use logical_entities::predicates::*;
use logical_entities::relation::Relation;
use logical_entities::row::Row;
use logical_entities::schema::Schema;
use physical_operators::Operator;
use type_system::borrowed_buffer::*;
use type_system::*;

use super::storage::StorageManager;

pub struct Project {
    input_relation: Relation,
    output_relation: Relation,
    predicate: Box<Predicate>,
}

impl Project {
    pub fn new(
        input_relation: Relation,
        output_cols: Vec<Column>,
        predicate: Box<Predicate>,
    ) -> Self {
        let schema = Schema::new(output_cols);
        let output_relation =
            Relation::new(&format!("{}_project", input_relation.get_name()), schema);

        Project {
            input_relation,
            output_relation,
            predicate,
        }
    }

    pub fn pure_project(relation: Relation, output_cols: Vec<Column>) -> Self {
        Self::new(relation, output_cols, Box::new(Tautology::new()))
    }
}

impl Operator for Project {
    fn get_target_relation(&self) -> Relation {
        self.output_relation.clone()
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Relation, String> {
        let in_schema = self.input_relation.get_schema();
        let in_schema_sizes = in_schema.to_size_vec();
        let in_record = storage_manager
            .get_with_schema(self.input_relation.get_name(), &in_schema_sizes)
            .unwrap();
        let out_schema = self.output_relation.get_schema();

        // Indices of the output columns in the input relation
        let mut out_cols_i = vec![];
        for col in out_schema.get_columns() {
            let i = in_schema
                .get_columns()
                .iter()
                .position(|x| x == col)
                .unwrap();
            out_cols_i.push(i);
        }

        for in_block in in_record.blocks() {
            for row_i in 0..in_block.len() {
                // Assemble values in the current row (this is very inefficient!)
                let mut values: Vec<Box<Value>> = vec![];
                for col_i in 0..in_schema.get_columns().len() {
                    let data = in_block.get_row_col(row_i, col_i).unwrap();
                    let data_type = in_schema.get_columns()[col_i].data_type();
                    let buff = BorrowedBuffer::new(&data, data_type, false);
                    values.push(buff.marshall());
                }

                // Check whether the current row satisfies the predicate
                let row = Row::new(in_schema.clone(), values);
                if !self.predicate.evaluate(&row) {
                    continue;
                }

                // Remap values to the order they appear in the output schema
                for col_i in 0..out_schema.get_columns().len() {
                    let data = in_block.get_row_col(row_i, out_cols_i[col_i]).unwrap();
                    storage_manager.append(self.output_relation.get_name(), data);
                }
            }
        }

        Ok(self.get_target_relation())
    }
}

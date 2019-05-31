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
    fn get_target_relation(&self) -> Option<Relation> {
        Some(self.output_relation.clone())
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Option<Relation>, String> {
        let in_schema = self.input_relation.get_schema();
        let in_physical_relation = storage_manager
            .relational_engine()
            .get(self.input_relation.get_name())
            .unwrap();
        let out_schema = self.output_relation.get_schema();

        storage_manager.relational_engine().drop(self.output_relation.get_name());
        let out_physical_relation = storage_manager.relational_engine().create(
            self.output_relation.get_name(),
            self.output_relation.get_schema().to_size_vec()
        );

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

        for in_block in in_physical_relation.blocks() {
            for row_i in 0..in_block.get_n_rows() {
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
                let mut row_builder = out_physical_relation.insert_row();
                for col_i in 0..out_schema.get_columns().len() {
                    let data = in_block.get_row_col(row_i, out_cols_i[col_i]).unwrap();
                    row_builder.push(data);
                }
            }
        }

        Ok(self.get_target_relation())
    }
}

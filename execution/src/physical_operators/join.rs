use logical_entities::column::Column;
use logical_entities::relation::Relation;
use logical_entities::schema::Schema;
use physical_operators::Operator;
use type_system::borrowed_buffer::BorrowedBuffer;
use type_system::Buffer;

use super::storage::StorageManager;

pub struct Join {
    relation_l: Relation,
    relation_r: Relation,
    l_columns: Vec<Column>,
    r_columns: Vec<Column>,
    output_relation: Relation,
}

impl Join {
    pub fn new(
        relation_l: Relation,
        relation_r: Relation,
        l_columns: Vec<Column>,
        r_columns: Vec<Column>,
    ) -> Self {
        let mut joined_cols = vec![];
        joined_cols.extend(relation_l.get_columns().clone());
        joined_cols.extend(relation_r.get_columns().clone());
        let output_relation = Relation::new(
            &format!("{}_j_{}", relation_l.get_name(), relation_r.get_name()),
            Schema::new(joined_cols),
        );
        Join {
            relation_l,
            relation_r,
            l_columns,
            r_columns,
            output_relation,
        }
    }
}

impl Operator for Join {
    fn get_target_relation(&self) -> Relation {
        self.output_relation.clone()
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Relation, String> {
        let l_schema = self.relation_l.get_schema();
        let l_columns = l_schema.get_columns();
        let l_physical_relation = storage_manager
            .relational_engine()
            .get(self.relation_l.get_name())
            .unwrap();

        let r_schema = self.relation_r.get_schema();
        let r_columns = r_schema.get_columns();
        let r_physical_relation = storage_manager
            .relational_engine()
            .get(self.relation_r.get_name())
            .unwrap();

        storage_manager.relational_engine().drop(self.output_relation.get_name());
        let out_physical_relation = storage_manager.relational_engine().create(
            self.output_relation.get_name(),
            self.output_relation.get_schema().to_size_vec()
        );

        let mut l_cols_i = vec![];
        for col in &self.l_columns {
            l_cols_i.push(l_columns.iter().position(|x| x == col).unwrap());
        }
        let mut r_cols_i = vec![];
        for col in &self.r_columns {
            r_cols_i.push(r_columns.iter().position(|x| x == col).unwrap());
        }

        for l_block in l_physical_relation.blocks() {
            for l_row_i in 0..l_block.get_n_rows() {
                let mut l_values = vec![];
                for l_col_i in &l_cols_i {
                    let l_data = l_block.get_row_col(l_row_i, *l_col_i).unwrap();
                    let l_data_type = l_columns[*l_col_i].data_type();
                    let l_buff = BorrowedBuffer::new(l_data, l_data_type, false);
                    l_values.push(l_buff.marshall());
                }

                for r_block in r_physical_relation.blocks() {
                    for r_row_i in 0..r_block.get_n_rows() {
                        let mut r_values = vec![];
                        for r_col_i in &r_cols_i {
                            let r_data = r_block.get_row_col(r_row_i, *r_col_i).unwrap();
                            let r_data_type = r_columns[*r_col_i].data_type();
                            let r_buff = BorrowedBuffer::new(r_data, r_data_type, false);
                            r_values.push(r_buff.marshall());
                        }

                        let mut include = true;
                        for i in 0..l_values.len() {
                            if !l_values[i].equals(&*r_values[i]) {
                                include = false;
                                break;
                            }
                        }
                        if !include {
                            continue;
                        }

                        let mut row_builder = out_physical_relation.insert_row();
                        for col_i in 0..l_columns.len() {
                            let data = l_block.get_row_col(l_row_i, col_i).unwrap();
                            row_builder.push(data);
                        }
                        for col_i in 0..r_columns.len() {
                            let data = r_block.get_row_col(r_row_i, col_i).unwrap();
                            row_builder.push(data);
                        }
                    }
                }
            }
        }

        Ok(self.get_target_relation())
    }
}

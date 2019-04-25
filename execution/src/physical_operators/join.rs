// Currently assumes fixed length 8 byte values.  Need to refactor to use value concepts.

use logical_entities::relation::Relation;
use logical_entities::schema::Schema;
use physical_operators::Operator;

use super::storage::StorageManager;

pub struct Join {
    relation_left: Relation,
    relation_right: Relation,
}

impl Join {
    pub fn new(relation_left: Relation, relation_right: Relation) -> Self {
        Join {
            relation_left,
            relation_right,
        }
    }
}

impl Operator for Join {
    fn get_target_relation(&self) -> Relation {
        let mut joined_cols = self.relation_left.get_columns().clone();
        joined_cols.extend(self.relation_right.get_columns().clone());

        Relation::new(
            &format!(
                "{}_j_{}",
                &self.relation_left.get_name(),
                &self.relation_right.get_name()
            ),
            Schema::new(joined_cols),
        )
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Relation, String> {
        let rel_l = &self.relation_left;
        let rel_r = &self.relation_right;
        let rel_l_size = rel_l.get_total_size(storage_manager);
        let rel_r_size = rel_r.get_total_size(storage_manager);
        let cols_l = rel_l.get_columns();
        let cols_r = rel_r.get_columns();
        let rows_l = rel_l_size / rel_l.get_row_size();
        let rows_r = rel_r_size / rel_r.get_row_size();
        let rows_l_size = rel_l.get_row_size();
        let rows_r_size = rel_r.get_row_size();

        let data_l = storage_manager.get(self.relation_left.get_name()).unwrap();
        let data_r = storage_manager.get(self.relation_right.get_name()).unwrap();

        // Future optimization: create uninitialized Vec (this may require unsafe Rust)
        let output_size = (rows_l * rows_r) * (rows_l_size + rows_r_size);
        let mut data_o: Vec<u8> = vec![0; output_size];

        let mut n: usize = 0;

        let mut i_l = 0;
        let mut i_r = 0;

        while i_l < rows_l {
            while i_r < rows_r {
                let mut col_offset_l = 0;
                for col_l in cols_l.iter() {
                    let mut v_l: Vec<u8> = vec![0; col_l.get_size()];
                    v_l.clone_from_slice(
                        &data_l[col_offset_l + i_l * rows_l_size
                            ..col_offset_l + i_l * rows_l_size + col_l.get_size()],
                    );
                    col_offset_l += col_l.get_size();

                    data_o[n..n + col_l.get_size()].clone_from_slice(&v_l); // 0  8
                    n += col_l.get_size();
                }

                let mut col_offset_r = 0;
                for col_r in cols_r.iter() {
                    let mut v_r: Vec<u8> = vec![0; col_r.get_size()];
                    v_r.clone_from_slice(
                        &data_r[col_offset_r + i_r * rows_r_size
                            ..col_offset_r + i_r * rows_r_size + col_r.get_size()],
                    );
                    col_offset_r += col_r.get_size();

                    data_o[n..n + col_r.get_size()].clone_from_slice(&v_r); // 0  8
                    n += col_r.get_size();
                }
                i_r += 1;
            }
            i_l += 1;
            i_r = 0;
        }

        storage_manager.put(self.get_target_relation().get_name(), &data_o);

        Ok(self.get_target_relation())
    }
}

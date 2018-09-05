use logical_entities::relation::Relation;
use logical_entities::schema::Schema;

use std::time::{Instant};

extern crate memmap;
use std::{
    fs::OpenOptions,
    io::{Seek, SeekFrom, Write},
};

#[derive(Debug)]
pub struct Join {
    relation_left: Relation,
    relation_right: Relation,
}

impl Join {
    pub fn new(relation_left: Relation, relation_right: Relation) -> Self {
        Join {
            relation_left,relation_right
        }
    }


    pub fn execute(&self) -> Relation {
        let now = Instant::now();

        let rel_l = &self.relation_left;
        let rel_r = &self.relation_right;
        let rel_l_size = rel_l.get_total_size();
        let rel_r_size = rel_r.get_total_size();
        let cols_l = rel_l.get_columns();
        let cols_r = rel_r.get_columns();
        let rows_l = rel_l_size/ rel_l.get_row_size();
        let rows_r = rel_r_size/ rel_r.get_row_size();
        let rows_l_size = rel_l.get_row_size();
        let rows_r_size = rel_r.get_row_size();

        let mut joined_cols = cols_l.clone();
        joined_cols.extend(cols_r.clone());

        let _join_relation = Relation::new(format!("{}_j_{}", rel_l.get_name(), rel_r.get_name()),
                                           Schema::new(joined_cols));


        let f_l = OpenOptions::new()
            .read(true)
            .open(rel_l.get_filename())
            .expect("Unable to open file");

        let data_l = unsafe {
            memmap::MmapOptions::new()
                .len(rel_l_size)
                .offset(0)
                .map(&f_l)
                .expect("Could not access data from memory mapped file")
        };

        let f_r = OpenOptions::new()
            .read(true)
            .open(rel_r.get_filename())
            .expect("Unable to open file");

        let data_r = unsafe {
            memmap::MmapOptions::new()
                .len(rel_r_size)
                .offset(0)
                .map(&f_r)
                .expect("Could not access data from memory mapped file")
        };

        let mut f_o = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(_join_relation.get_filename())
            .expect("Unable to open file");

        // Allocate space in the file first
        f_o.seek(SeekFrom::Start(((rows_l * rows_r) * (rows_l_size + rows_r_size)) as u64 )).unwrap();
        f_o.write_all(&[0]).unwrap();
        f_o.seek(SeekFrom::Start(0)).unwrap();

        let mut data_o = unsafe {
            memmap::MmapOptions::new()
                .map_mut(&f_o)
                .expect("Could not access data from memory mapped file")
        };

        let mut n: usize = 0;

        let mut v_l : [u8; 8] = [0,0,0,0,0,0,0,0];
        let mut v_r : [u8; 8] = [0,0,0,0,0,0,0,0];

        let mut i_l = 0;
        let mut i_r = 0;

        while i_l < rows_l {

            while i_r < rows_r {
                let mut col_offset_l = 0;
                for col_l in cols_l.iter() {
                    v_l.clone_from_slice(&data_l[col_offset_l + i_l*rows_l_size.. col_offset_l + i_l*rows_l_size + col_l.get_size()]);
                    col_offset_l += col_l.get_size();

                    data_o[n..n + col_l.get_size()].clone_from_slice(&v_l); // 0  8
                    n = n + col_l.get_size();
                }

                let mut col_offset_r = 0;
                for col_r in cols_r.iter() {
                    v_r.clone_from_slice(&data_r[col_offset_r + i_r*rows_r_size.. col_offset_r + i_r*rows_r_size + col_r.get_size()]);
                    col_offset_r += col_r.get_size();

                    data_o[n..n + col_r.get_size()].clone_from_slice(&v_r); // 0  8
                    n = n + col_r.get_size();
                }
                i_r+=1;
            }
            i_l+=1;
            i_r = 0;
        }
        println!("Finished Join After {} seconds.", now.elapsed().as_secs());
        _join_relation
    }
}
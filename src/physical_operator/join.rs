use logical_operator::logical_relation::LogicalRelation;

use std::time::{Instant};

extern crate memmap;
use std::mem;
use std::{
    fs::OpenOptions,
};

#[derive(Debug)]
pub struct Join {
    relation_left: LogicalRelation,
    relation_right: LogicalRelation,
}

impl Join {
    pub fn new(relation_left: LogicalRelation, relation_right: LogicalRelation) -> Self {
        Join {
            relation_left,relation_right
        }
    }


    pub fn execute(&self) -> bool{
        let now = Instant::now();



        let rel_l = &self.relation_left;
        let rel_r = &self.relation_right;
        let rel_l_size = rel_l.get_total_size();
        let rel_r_size = rel_r.get_total_size();
        let cols_l = rel_l.get_columns();
        let cols_r = rel_r.get_columns();
        let rows_l = rel_l_size/ rel_l.get_row_size();
        let rows_r = rel_r_size/ rel_r.get_row_size();

        let mut joined_cols = cols_l.clone();
        joined_cols.extend(cols_r.clone());

        let _join_relation = LogicalRelation::new(format!("{}_j_{}",rel_l.get_name(),rel_r.get_name()),
                                                                joined_cols);


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

        let mut v_l : [u8; 8] = [0,0,0,0,0,0,0,0];
        let mut v_r : [u8; 8] = [0,0,0,0,0,0,0,0];

        let mut i_l = 0;
        let mut i_r = 0;
        while i_l < rows_l {
            while i_r < rows_r {
                for col_l in cols_l {
                    v_l.clone_from_slice(&data_l[i_l..i_l + col_l.get_size()]);
                    unsafe {
                        let _v_l_t = mem::transmute::<[u8; 8], u64>(v_l);
                        //Write to joined relation
                    }
                }

                for col_r in cols_r {
                    v_r.clone_from_slice(&data_r[i_r..i_r + col_r.get_size()]);
                    unsafe {
                        let _v_r_t = mem::transmute::<[u8; 8], u64>(v_r);
                        //Write to joined relation
                    }
                }
                i_r+=1;
            }
            i_l+=1;
        }
        println!("Finished Reading MemMap After {} milli-seconds.", now.elapsed().subsec_millis());
        true
    }
}
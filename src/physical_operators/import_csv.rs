use logical_entities::relation::Relation;
use logical_entities::types::DataTypeTrait;

use std::time::{Instant};

extern crate memmap;
use std::{
    fs::OpenOptions,
    io::{Seek, SeekFrom, Write},
};

#[derive(Debug)]
pub struct ImportCsv {
    file_name: String,
    relation: Relation
}

impl ImportCsv {
    pub fn new(file_name: String, relation: Relation) -> Self {
        ImportCsv {
            file_name,relation
        }
    }

    pub fn execute(&self) -> bool{
        let now = Instant::now();

        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.relation.get_filename())
            .expect("Unable to open file");

        extern crate csv;
        let mut rdr = csv::Reader::from_path(&self.file_name).unwrap();
        let record_count = rdr.records().count() + 1;
        rdr.seek(csv::Position::new()).unwrap();

        // Allocate space in the file first
        f.seek(SeekFrom::Start((self.relation.get_row_size() * record_count) as u64)).unwrap();
        f.write_all(&[0]).unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();

        f.set_len((self.relation.get_row_size() * record_count) as u64).unwrap();

        let mut data = unsafe {
            memmap::MmapOptions::new()
                .map_mut(&f)
                .expect("Could not access data from memory mapped file")
        };

        let columns = self.relation.get_columns();
        let mut n : usize = 0;

        let mut process_record = |data: &mut memmap::MmapMut, record: &csv::StringRecord| {
            for (i, column) in columns.iter().enumerate() {

                let a = record.get(i).unwrap().to_string();

                let (c,size) = column.get_datatype().parse_and_marshall(a);
                data[n..n + size].clone_from_slice(&c); // 0  8
                n = n + size;
            }
        };


        //process_record(&mut data,rdr.headers().unwrap());

        for result in rdr.records() {
            let record = result.unwrap();

            process_record(&mut data,&record);
        }
        println!("Finished CSV to Hustle load in {} Seconds.", now.elapsed().as_secs());
        true
    }

    pub fn u8s_from_u64(a: &mut [u8], v: u64) {
        a[0] = v as u8;
        a[1] = (v >> 8) as u8;
        a[2] = (v >> 16) as u8;
        a[3] = (v >> 24) as u8;
        a[4] = (v >> 32) as u8;
        a[5] = (v >> 40) as u8;
        a[6] = (v >> 48) as u8;
        a[7] = (v >> 56) as u8;
    }
}
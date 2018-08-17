use logical_operator::logical_relation::LogicalRelation;

use std::time::{Instant};

extern crate memmap;
use std::mem;
use std::{
    fs::OpenOptions,
    io::{Seek, SeekFrom, Write},
};

#[derive(Debug)]
pub struct ImportCsv {
    file_name: String,
    relation: LogicalRelation
}

impl ImportCsv {
    pub fn new(file_name: String, relation: LogicalRelation) -> Self {
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
            .open(self.relation.to_filename())
            .expect("Unable to open file");

        // Allocate space in the file first
        f.seek(SeekFrom::Start(self.relation.get_total_size() as u64)).unwrap();
        f.write_all(&[0]).unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();



        let mut data = unsafe {
            memmap::MmapOptions::new()
                .map_mut(&f)
                .expect("Could not access data from memory mapped file")
        };

        extern crate csv;
        let mut rdr = csv::Reader::from_path(&self.file_name).unwrap();
        let columns = self.relation.get_columns();
        let mut n : usize = 0;

        let mut process_record = |data: &mut memmap::MmapMut, record: &csv::StringRecord| {
            for (i, column) in columns.iter().enumerate() {
                let a = record.get(i).unwrap().parse::<u64>().unwrap();
                unsafe {
                    //let c = mem::transmute::<u64, [u8; 8]>(a);
                    use std::slice;
                    let ip: *const u64 = &a;
                    let bp: *const u8 = ip as *const _;
                    let bs: &[u8] = unsafe { slice::from_raw_parts(bp, mem::size_of::<u64>()) };
                    let d:&[u8] =slice::from_raw_parts(bp, 8);//column.get_size());
                    data[n..n + column.get_size()].clone_from_slice(d); // 0  8
                    n = n + column.get_size();
                }
            }
        };


        process_record(&mut data,rdr.headers().unwrap());

        for result in rdr.records() {
            let record = result.unwrap();
            process_record(&mut data,&record);
        }
        println!("Finished CSV to Hustle load in {} Seconds.", now.elapsed().as_secs());
        true
    }
}
extern crate rand;
extern crate memmap;
extern crate sqlite;
extern crate csv;

use std::mem;

use std::time::{Instant};

use std::thread;

use std::{
    fs::OpenOptions,
    io::{Seek, SeekFrom, Write},
};

use rand::Rng;

const CHUNK_SIZE: usize = 1024*1024;
const CHUNK_COUNT: usize = 1024;
const SIZE: usize = CHUNK_SIZE * CHUNK_COUNT;
const RUN_DATA_GEN: bool = false;

fn generate_csv(count: usize, filename: String) {
    let flush_every_rows = 1024*1024;
    let outer_count = count/flush_every_rows;

    let mut rng = rand::thread_rng();

    let mut wtr = csv::Writer::from_path(filename).unwrap();
    wtr.write_record(&["a"]).unwrap();
    for y in 1..outer_count {
        for n in 1..flush_every_rows {
            let x:u64 = rng.gen();
            wtr.write_record(&[x.to_string()]).unwrap();
        }
        println!("Writing {}/{}", y,outer_count);
        wtr.flush().unwrap();
    }
    wtr.flush().unwrap();
}

fn main(){
    generate_csv(SIZE, "data.csv".to_string())
}

/*
fn main2() {




    if RUN_DATA_GEN {
        let connection = sqlite::open("sqlite.data").unwrap();
        connection
            .execute(
                "DROP TABLE IF EXISTS t;"
            ).unwrap();

        connection
            .execute(
                "CREATE TABLE t (a INTEGER);"
            )
            .unwrap();

        {
            /* Write */
            let now = Instant::now();
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open("test.mmap")
                .expect("Unable to open file");

            // Allocate space in the file first
            f.seek(SeekFrom::Start((SIZE as u64)*8)).unwrap();
            f.write_all(&[0]).unwrap();
            f.seek(SeekFrom::Start(0)).unwrap();


            let mut data = unsafe {
                memmap::MmapOptions::new()
                    .map_mut(&f)
                    .expect("Could not access data from memory mapped file")
            };

            println!("Finished Buffering File After {} Seconds.", now.elapsed().as_secs());

            for n in 1..SIZE {
                let x = rand::random::<u64>();
                unsafe {
                    let b = mem::transmute::<u64,[u8; 8]>(x);
                    data[n..n+8].clone_from_slice(&b);
                }
                //connection.execute(format!("INSERT INTO t (a) VALUES ({});", x)).unwrap();
            }
            println!("Finished Writing After {} Seconds.", now.elapsed().as_secs());
        }
    }


    { /* Read */
        let now = Instant::now();
        let mut children = vec![];

        for mut c in 0..CHUNK_COUNT {
            children.push(thread::spawn(move|| {

                let f = OpenOptions::new()
                    .read(true)
                    .open("test.mmap")
                    .expect("Unable to open file");

                let data = unsafe {
                    memmap::MmapOptions::new()
                        .len(CHUNK_SIZE)
                        .offset(c * CHUNK_SIZE)
                        .map(&f)
                        .expect("Could not access data from memory mapped file")
                };

                let mut t :u64 = 0;
                for i in 1..CHUNK_SIZE {
                    t += mem::transmute::<[u8; 8], u64>(data[i]);
                    t += data[i] as u64;
                }
                t
            }));
        }

        let mut intermediate_sums = vec![];
        for child in children {
            let intermediate_sum = child.join().unwrap();
            intermediate_sums.push(intermediate_sum);
        }
        let final_result = intermediate_sums.iter().sum::<u64>();
        println!("Final Sum: {}", final_result);
        println!("Finished Reading MemMap After {} milli-seconds.", now.elapsed().subsec_millis());
    }

    {
        let connection = sqlite::open("sqlite.data").unwrap();
        let now = Instant::now();
        connection
            .iterate("SELECT SUM(a) FROM t;", |pairs| {
                for &(column, value) in pairs.iter() {
                    println!("{} = {}", column, value.expect("Value not valid."));
                }
                true
            })
            .expect("Query failed.");
        println!("Finished SQL Sum After {} milli-seconds.", now.elapsed().subsec_millis());
    }

}*/
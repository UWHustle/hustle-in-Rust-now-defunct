extern crate rand;
extern crate csv;

use rand::Rng;

const SIZE: usize = 1024*1024*1024;

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

extern crate hustle_storage;

#[macro_use]
extern crate criterion;
extern crate memmap;
extern crate rand;

use hustle_storage::StorageManager;
use criterion::{Criterion, Benchmark};
use std::{thread, fs};
use std::sync::Arc;
use std::path::Path;
use std::fs::File;
use memmap::Mmap;
use rand::Rng;

const VALUE_SIZE: usize = 1000;

//fn put(threads: u16) {
//    let sm = Arc::new(StorageManager::new());
//    let mut handles = vec![];
//
//    for t in 0..threads {
//        let sm = Arc::clone(&sm);
//        let handle = thread::spawn(move || {
//            for p in 0..10 {
//                let key = format!("{}{}", t, p);
//                let value: [u8; VALUE_SIZE] = [0; VALUE_SIZE];
//                sm.key_value_engine().put(key.as_str(), &value);
//                sm.key_value_engine().delete(key.as_str());
//            }
//        });
//        handles.push(handle);
//    }
//
//    for handle in handles {
//        handle.join().unwrap();
//    }
//}

fn mmap() {
    let path = Path::new("testfile_10k");
    let file = File::open(path).unwrap();
    let _buf = unsafe { Mmap::map(&file).unwrap() };
}

fn read() {
    let path = Path::new("testfile_10k");
    let _buf = fs::read(path).unwrap();
}

fn sequential(buf: &[u8]) {
    let mut offset = 0;
    let size = 1000;
    while offset + size < buf.len() {
        let _buf_slice = &buf[offset .. offset + size];
        offset += size;
    }
}

fn random(buf: &[u8]) {
    let mut rng = rand::thread_rng();
    let size = 1000;
    let max_offset = buf.len() - size;
    for _ in 0 .. buf.len() / size {
        let offset = rng.gen_range(0, max_offset);
        let _buf_slice = &buf[offset .. offset + size];
    }
}

fn bench(c: &mut Criterion) {
//    c.bench_function("put", |b| b.iter(|| put(4)));

    c.bench(
        "storage_manager",
        Benchmark::new(
            "mmap_initialize",
            |b| b.iter(|| mmap())
        )
    );

    c.bench(
        "storage_manager",
        Benchmark::new(
            "read_initialize",
            |b| b.iter(|| read())
        )
    );

    c.bench(
        "storage_manager",
        Benchmark::new(
            "mmap_sequential",
            |b| {
                let path = Path::new("testfile_100m");
                let file = File::open(path).unwrap();
                let buf = unsafe { Mmap::map(&file).unwrap() };
                b.iter(|| sequential(buf.as_ref()))
            }
        )
    );

    c.bench(
        "storage_manager",
        Benchmark::new(
            "read_sequential",
            |b| {
                let path = Path::new("testfile_100m");
                let buf = fs::read(path).unwrap();
                b.iter(|| sequential(buf.as_ref()))
            }
        )
    );

    c.bench(
        "storage_manager",
        Benchmark::new(
            "mmap_random",
            |b| {
                let path = Path::new("testfile_100m");
                let file = File::open(path).unwrap();
                let buf = unsafe { Mmap::map(&file).unwrap() };
                b.iter(|| random(buf.as_ref()))
            }
        )
    );

    c.bench(
        "storage_manager",
        Benchmark::new(
            "read_sequential",
            |b| {
                let path = Path::new("testfile_100m");
                let buf = fs::read(path).unwrap();
                b.iter(|| random(buf.as_ref()))
            }
        )
    );
}

criterion_group!(benches, bench);
criterion_main!(benches);

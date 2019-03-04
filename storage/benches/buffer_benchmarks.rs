extern crate storage;

#[macro_use]
extern crate criterion;

use storage::buffer::Buffer;
use criterion::Criterion;
use std::thread;
use std::sync::Arc;

const PAGE_SIZE: usize = 1000;

fn put(capacity: usize, threads: u16) {
    let buffer = Arc::new(Buffer::with_capacity(capacity));
    let mut handles = vec![];

    for t in 0..threads {
        let buffer = Arc::clone(&buffer);
        let handle = thread::spawn(move || {
            for p in 0..10 {
                let key = format!("{}{}", t, p);
                let page: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
                buffer.put(key.as_str(), &page);
                buffer.delete(key.as_str());
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

fn bench(c: &mut Criterion) {
    c.bench_function("put", |b| b.iter(|| put(10, 4)));
}

criterion_group!(benches, bench);
criterion_main!(benches);

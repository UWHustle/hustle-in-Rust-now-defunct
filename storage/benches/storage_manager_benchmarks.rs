extern crate storage;

#[macro_use]
extern crate criterion;

use storage::StorageManager;
use criterion::Criterion;
use std::thread;
use std::sync::Arc;

const VALUE_SIZE: usize = 1000;

fn put(threads: u16) {
    let sm = Arc::new(StorageManager::new());
    let mut handles = vec![];

    for t in 0..threads {
        let sm = Arc::clone(&sm);
        let handle = thread::spawn(move || {
            for p in 0..10 {
                let key = format!("{}{}", t, p);
                let value: [u8; VALUE_SIZE] = [0; VALUE_SIZE];
                sm.put(key.as_str(), &value);
                sm.delete(key.as_str());
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

fn bench(c: &mut Criterion) {
    c.bench_function("put", |b| b.iter(|| put(4)));
}

criterion_group!(benches, bench);
criterion_main!(benches);

#[macro_use]
extern crate lazy_static;

use std::time::{Instant, Duration};
use std::collections::HashMap;
use std::sync::Mutex;

lazy_static! {
    static ref GLOBAL_START: Instant = Instant::now();
    static ref STARTS: Mutex<HashMap<&'static str, Duration>> = Mutex::new(HashMap::new());
    static ref SPANS: Mutex<Vec<Span>> = Mutex::new(vec![]);
}

pub fn start(key: &'static str) {
    STARTS.lock().unwrap()
        .insert(key, GLOBAL_START.elapsed())
        .map(|_| panic!("Called start twice with the key \"{}\"", key));
}

pub fn end(key: &'static str) {
    let start = STARTS.lock().unwrap()
        .remove(key)
        .unwrap_or_else(|| panic!("Called end before start with key \"{}\"", key));

    SPANS.lock().unwrap()
        .push(Span { key, start, end: GLOBAL_START.elapsed() })
}

pub fn dump() {
    STARTS.lock().unwrap().clear();
    for span in SPANS.lock().unwrap().drain(..) {
        println!("{}, {}, {}", span.key, span.start.as_millis(), span.end.as_millis());
    }
}

struct Span {
    key: &'static str,
    start: Duration,
    end: Duration
}

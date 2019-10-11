//use core_affinity::CoreId;
//use std::sync::mpsc::{Receiver};
////use crossbeam::thread;
//use crate::operator::WorkOrder;
//use std::thread;
//use std::sync::{Arc, Mutex};
//
//pub struct Worker {
//    id: usize,
//    core_id: CoreId,
//    worker_states: Arc<Mutex<Vec<bool>>>,
//    rx: Receiver<Box<dyn WorkOrder>>,
//}
//
//impl Worker {
//    pub fn new(
//        id: usize,
//        core_id: CoreId,
//        worker_states: Arc<Mutex<Vec<bool>>>,
//        rx: Receiver<Box<dyn WorkOrder>>
//    ) {
//        let mut worker = Worker {
//            id,
//            core_id,
//            worker_states,
//            rx,
//        };
//        worker.run();
//    }
//
//    fn run(&mut self) {
//        thread::spawn(move || {
//            core_affinity::set_for_current(self.core_id);
//            loop {
//                match self.rx.recv() {
//                    Ok(work_order) => {
//                        work_order.execute();
//                        self.worker_states.lock().unwrap()[self.id] = false;
//                    }
//                    Err(_e) => break
//                }
//            }
//        });
//    }
//}
//





use crate::operator::WorkOrder;
use core_affinity::CoreId;
use std::sync::mpsc::{Receiver};
use std::thread;
use std::sync::{Arc, Mutex};

pub struct Worker {
//    id: usize,
//    core_id: CoreId,
//    worker_states: Arc<Mutex<Vec<bool>>>,
//    rx: Receiver<Box<dyn WorkOrder>>,
}

impl Worker {
    pub fn new(
        id: usize,
        core_id: CoreId,
        worker_states: Arc<Mutex<Vec<bool>>>,
        rx: Receiver<Box<dyn WorkOrder>>
    ) {
        thread::spawn(move || {
            core_affinity::set_for_current(core_id);
            loop {
                match rx.recv() {
                    Ok(work_order) => {
                        println!("id: {}", id);
                        work_order.execute();
                        worker_states.lock().unwrap()[id] = false;
                    }
                    Err(_e) => break
                }
            }
        });
    }
}


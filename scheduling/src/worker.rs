use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;

use hustle_common::Message;
use hustle_storage::StorageManager;

pub struct Worker {
    id: usize,
    worker_rx: Receiver<Vec<u8>>,
    scheduler_tx: Sender<Vec<u8>>,
    storage_manager: Arc<StorageManager>,
}

impl Worker {
    pub fn new(
        id: usize,
        worker_rx: Receiver<Vec<u8>>,
        scheduler_tx: Sender<Vec<u8>>,
        storage_manager: Arc<StorageManager>,
    ) -> Self {
        Worker {
            id,
            worker_rx,
            scheduler_tx,
            storage_manager,
        }
    }

    pub fn run(&mut self) {
        loop {
            if let Ok(msg) = self.worker_rx.recv() {
                // println!("worker {} received work order", self.id);
                let request = Message::deserialize(&msg).unwrap();
                let response = match request {
                    Message::WorkOrder {
                        query_id,
                        op_index,
                        work_order,
                        is_normal_work_order,
                    } => {
                        let mut work_order: Box<Box<dyn hustle_operators::WorkOrder>> =
                            unsafe { Box::from_raw(work_order as *mut _) };
                        work_order.execute(Arc::clone(&self.storage_manager));
                        Message::WorkOrderCompletion {
                            query_id,
                            op_index,
                            is_normal_work_order,
                            worker_id: self.id,
                        }
                    }
                    _ => request,
                };
                self.scheduler_tx
                    .send(response.serialize().unwrap())
                    .unwrap();
            } else {
                break;
            }
        }
    }
}

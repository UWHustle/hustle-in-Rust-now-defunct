use std::net::TcpStream;
use message::Message;
use core::borrow::BorrowMut;
use std::sync::mpsc::{Receiver, Sender};

pub struct HustleConnection {
    id: u64,
    tcp_stream: TcpStream,
}

impl HustleConnection {
    pub fn new(id: u64, tcp_stream: TcpStream) -> Self {
        HustleConnection {
            id,
            tcp_stream,
        }
    }

    pub fn listen(&mut self, input_rx: Receiver<Vec<u8>>, output_tx: Sender<Vec<u8>>) {
        while let Ok(request) = Message::receive(self.tcp_stream.borrow_mut()) {

            let response = match request {
                Message::ExecuteSQL { sql } =>
                    Message::OptimizeSQL { sql, connection_id: self.id},
                _ => panic!("Invalid message type sent to connection")
            };

            // Pass on the message to the server.
            output_tx.send(response.serialize().unwrap()).unwrap();

            // Wait for the server's response.
            let result = Message::deserialize(&input_rx.recv().unwrap()).unwrap();

            match result {
                Message::ReturnRow { row, connection_id: _ } => {
                    println!("returned row! {}", row.len());
                },
                _ => panic!("Invalid message sent to connection")
            }
        }
    }
}

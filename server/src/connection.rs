use std::net::TcpStream;
use message::Message;
use std::sync::mpsc::{Receiver, Sender};

pub struct ServerConnection {
    id: u64,
    tcp_stream: TcpStream,
}

impl ServerConnection {
    pub fn new(id: u64, tcp_stream: TcpStream) -> Self {
        ServerConnection {
            id,
            tcp_stream,
        }
    }

    pub fn listen(&mut self, input_rx: Receiver<Vec<u8>>, output_tx: Sender<Vec<u8>>) {
        loop {
            let request = Message::receive(&mut self.tcp_stream).unwrap();
            let response = match request {
                Message::ExecuteSQL { sql } =>
                    Message::OptimizeSQL { sql, connection_id: self.id},
                _ => panic!("Invalid message type sent to connection")
            };

            // Pass on the message to Hustle.
            output_tx.send(response.serialize().unwrap()).unwrap();

            // Wait for the response.
            loop {
                let result = Message::deserialize(&input_rx.recv().unwrap()).unwrap();
                result.send(&mut self.tcp_stream).unwrap();
                match result {
                    Message::Success { connection_id: _ } => break,
                    Message::Error { reason: _, connection_id: _ } => break,
                    _ => continue
                }
            }
        }
    }
}

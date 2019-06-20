use std::net::TcpStream;
use message::{Message, Listener};
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
}

impl Listener for ServerConnection {
    fn listen(&mut self, input_rx: Receiver<Vec<u8>>, output_tx: Sender<Vec<u8>>) {
        loop {
            if let Ok(request) = Message::receive(&mut self.tcp_stream) {
                if let Message::ExecuteSql { sql } = request {
                    // Pass on the message to Hustle.
                    output_tx.send(Message::ParseSql {
                        sql,
                        connection_id: self.id
                    }.serialize().unwrap()).unwrap();
                } else {
                    panic!("Invalid message type sent to connection")
                }

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
            } else {
                output_tx.send(Message::CloseConnection {
                    connection_id: self.id
                }.serialize().unwrap()).unwrap();
                break;
            }
        }
    }
}

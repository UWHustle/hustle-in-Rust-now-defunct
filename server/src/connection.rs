use std::net::TcpStream;
use hustle_common::Message;
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

    pub fn listen(
        &mut self,
        connection_rx: Receiver<Vec<u8>>,
        parser_tx: Sender<Vec<u8>>,
        transaction_tx: Sender<Vec<u8>>,
        completed_tx: Sender<Vec<u8>>,
    ) {
        loop {
            if let Ok(request) = Message::receive(&mut self.tcp_stream) {
                match request {
                    Message::ExecuteSql { sql } => parser_tx.send(Message::ParseSql {
                        sql,
                        connection_id: self.id
                    }.serialize().unwrap()).unwrap(),
                    _ => completed_tx.send(Message::Failure {
                        reason: "Invalid message type sent to connection".to_owned(),
                        connection_id: self.id
                    }.serialize().unwrap()).unwrap()
                };

                // Wait for the response.
                loop {
                    let result = Message::deserialize(&connection_rx.recv().unwrap()).unwrap();
                    result.send(&mut self.tcp_stream).unwrap();
                    match result {
                        Message::Success { connection_id: _ } => break,
                        Message::Failure { reason: _, connection_id: _ } => break,
                        _ => continue
                    }
                }
            } else {
                transaction_tx.send(Message::CloseConnection {
                    connection_id: self.id
                }.serialize().unwrap()).unwrap();
                break;
            }
        }
    }
}

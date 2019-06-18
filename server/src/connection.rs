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

    pub fn listen(
        &mut self,
        input_rx: Receiver<Vec<u8>>,
        optimizer_tx: Sender<Vec<u8>>,
        transaction_tx: Sender<Vec<u8>>
    ) {
        profiler::start("connection");

        loop {
            if let Ok(request) = Message::receive(&mut self.tcp_stream) {
                profiler::start("statement");

                if let Message::ExecuteSql { sql } = request {
                    // Pass on the message to Hustle.
                    optimizer_tx.send(Message::OptimizeAst {
                        ast: sql,
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

                profiler::end("statement");
            } else {
                transaction_tx.send(Message::CloseConnection {
                    connection_id: self.id
                }.serialize().unwrap()).unwrap();
                break;
            }
        }

        profiler::end("connection");
        profiler::dump();
    }
}

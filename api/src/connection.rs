use std::io;
use std::net::{TcpStream, ToSocketAddrs};

use crate::HustleResult;

pub struct HustleConnection {
    tcp_stream: TcpStream
}

impl HustleConnection {
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<HustleConnection, io::Error> {
        TcpStream::connect(addr).map(|tcp_stream| HustleConnection {
            tcp_stream
        })
    }

    pub fn execute(&mut self, sql: String) -> Result<Option<HustleResult>, String> {
        let request = Message::ExecuteSql { sql };
        request.send(&mut self.tcp_stream).unwrap();

        let response = Message::receive(&mut self.tcp_stream).unwrap();
        match response {
            Message::Success { connection_id: _ } => Ok(None),
            Message::Schema { schema, connection_id: _ } => {
                let mut rows = vec![];
                loop {
                    let response = Message::receive(&mut self.tcp_stream).unwrap();
                    match response {
                        Message::Success { connection_id: _ } => break,
                        Message::ReturnRow { row, connection_id: _ } => {
                            rows.push(row);
                        }
                        _ => panic!("Invalid message type sent to client")
                    };
                };
                Ok(Some(HustleResult::new(schema, rows)))
            },
            Message::Failure { reason, connection_id: _ } => Err(reason),
            _ => panic!("Invalid message type sent to client")
        }
    }
}

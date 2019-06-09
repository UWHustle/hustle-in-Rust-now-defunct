use std::net::{ToSocketAddrs, TcpStream};
use std::io;
use message::Message;
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

    pub fn execute(&mut self, sql: String) -> Option<HustleResult> {
        let request = Message::ExecuteSQL { sql };
        request.send(&mut self.tcp_stream).unwrap();

        let response = Message::receive(&mut self.tcp_stream).unwrap();
        match response {
            Message::Success { connection_id: _ } => None,
            Message::Schema { data_types, connection_id: _ } => {
                let mut rows = vec![];
                loop {
                    let response = Message::receive(&mut self.tcp_stream).unwrap();
                    match response {
                        Message::Success { connection_id: _ } => break,
                        Message::ReturnRow { row, connection_id: _ } => {
                            rows.push(row);
                        }
                        _ => panic!("Invalid message type sent to client")
                    }
                }
                Some(HustleResult::new(data_types, rows))
            },
            _ => panic!("Invalid message type sent to client")
        }

    }
}

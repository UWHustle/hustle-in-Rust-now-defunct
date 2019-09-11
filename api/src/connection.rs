use std::io;
use std::net::{TcpStream, ToSocketAddrs};

use crate::HustleResult;
use hustle_common::message::Message;

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
            Message::Success => Ok(None),
            Message::Schema { schema } => {
                let names = schema.iter()
                    .map(|c| c.get_name().to_owned())
                    .collect();

                let types = schema.into_iter()
                    .map(|c| c.into_type_variant().into_type())
                    .collect();

                let mut rows = vec![];
                loop {
                    let response = Message::receive(&mut self.tcp_stream).unwrap();
                    match response {
                        Message::Success => break,
                        Message::ReturnRow { row } => {
                            rows.push(row);
                        }
                        _ => panic!("Invalid message type sent to client")
                    };
                };
                Ok(Some(HustleResult::new(names, types, rows)))
            },
            Message::Failure { reason } => Err(reason),
            _ => panic!("Invalid message type sent to client")
        }
    }
}

use std::sync::mpsc::{Receiver, Sender};
use message::Message;

pub struct Parser {

}

impl Parser {
    pub fn new() -> Self {
        Parser {

        }
    }

    pub fn listen(
        &self,
        input_rx: Receiver<Vec<u8>>,
        optimizer_tx: Sender<Vec<u8>>,
        completed_tx: Sender<Vec<u8>>
    ) {
        loop {
            let request = Message::deserialize(&input_rx.recv().unwrap()).unwrap();
            if let Message::ParseSql { sql, connection_id } = request {
                let ast = hustle_parser::parse(&sql);
                optimizer_tx.send(Message::OptimizeAST {
                    ast,
                    connection_id
                })
            } else {
                panic!("Invalid message type sent to parser");
            }
        }
    }
}

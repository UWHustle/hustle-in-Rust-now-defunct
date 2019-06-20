extern crate message;

use std::ffi::CString;
use std::os::raw::c_char;
use std::sync::mpsc::{Receiver, Sender};

use message::{Listener, Message};

extern {
    fn c_parse(sql: *const c_char) -> *mut c_char;
}

pub fn parse(sql: &str) -> String {
    let c_sql = CString::new(sql).expect("Invalid SQL");
    unsafe {
        let c_ast_raw = c_parse(c_sql.as_ptr());
        let c_ast = CString::from_raw(c_ast_raw);
        c_ast.to_str()
            .expect("Could not convert parse result to owned String")
            .to_owned()
    }
}

pub struct Parser;

impl Parser {
    pub fn new() -> Self {
        Parser
    }
}

impl Listener for Parser {
    fn listen(&mut self, input_rx: Receiver<Vec<u8>>, output_tx: Sender<Vec<u8>>) {
        loop {
            let request = Message::deserialize(&input_rx.recv().unwrap()).unwrap();
            let response = match request {
                Message::ParseSql { sql, connection_id } => {
                    let ast = parse(&sql);
                    Message::ResolveAst { ast, connection_id }
                },
                _ => request
            };
            output_tx.send(response.serialize().unwrap()).unwrap();
        }
    }
}

extern crate message;

use std::ffi::{CString, CStr};
use std::os::raw::{c_char, c_int};
use std::sync::mpsc::{Receiver, Sender};
use self::message::{Listener, Message};
use std::ptr;

extern {
    fn c_parse(sql: *const c_char, result: &mut *const c_char) -> c_int;
}

pub fn parse(sql: &str) -> Result<String, String> {
    let c_sql = CString::new(sql).expect("Invalid SQL");
    let mut c_result = ptr::null();
    unsafe {
        let code = c_parse(c_sql.as_ptr(), &mut c_result);
        let result = CStr::from_ptr(c_result)
            .to_str()
            .map(|a| a.to_owned())
            .map_err(|e| e.to_string())?;
        match code {
            0 => Ok(result),
            _ => Err(result)
        }
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
                    match parse(&sql) {
                        Ok(ast) => Message::ResolveAst { ast, connection_id },
                        Err(reason) => Message::Failure { reason, connection_id }
                    }
                },
                _ => request
            };
            output_tx.send(response.serialize().unwrap()).unwrap();
        }
    }
}

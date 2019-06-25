extern crate message;

use std::ffi::{CString, CStr};
use std::os::raw::{c_char, c_int};
use std::sync::mpsc::{Receiver, Sender};
use self::message::Message;
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

    pub fn listen(
        &mut self,
        parser_rx: Receiver<Vec<u8>>,
        resolver_tx: Sender<Vec<u8>>,
        completed_tx: Sender<Vec<u8>>,
    ) {
        loop {
            let buf = parser_rx.recv().unwrap();
            let request = Message::deserialize(&buf).unwrap();
            match request {
                Message::ParseSql { sql, connection_id } => {
                    match parse(&sql) {
                        Ok(ast) => resolver_tx.send(Message::ResolveAst {
                            ast,
                            connection_id,
                        }.serialize().unwrap()).unwrap(),
                        Err(reason) => completed_tx.send(Message::Failure {
                            reason,
                            connection_id,
                        }.serialize().unwrap()).unwrap()
                    };
                },
                _ => completed_tx.send(buf).unwrap()
            };
        }
    }
}

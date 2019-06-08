use std::process::Command;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, Sender};
use message::Message;

pub struct Optimizer;

impl Optimizer {
    pub fn new() -> Self {
        Optimizer
    }

    pub fn listen(&mut self, input_rx: Receiver<Vec<u8>>, output_tx: Sender<Vec<u8>>) {
        loop {
            let buf = input_rx.recv().unwrap();
            let request = Message::deserialize(&buf).unwrap();
            let response = match request {
                Message::OptimizeSQL { mut sql, connection_id } => {
                    sql.make_ascii_lowercase();

                    // TODO: Add parser, optimizer support for transactions.
                    // Currently, the optimizer does not support transaction keywords, so we
                    // check for them manually here.
                    if sql.contains("begin") {
                        Message::BeginTransaction { connection_id }
                    } else if sql.contains("commit") {
                        Message::CommitTransaction { connection_id }
                    } else {
                        let plan = self.optimize(&sql).unwrap();
                        Message::ExecutePlan { plan, connection_id }
                    }
                },
                _ => panic!("Invalid message type sent to optimizer")
            };
            output_tx.send(response.serialize().unwrap()).unwrap();
        }
    }

    pub fn optimize(&self, sql: &str) -> Result<String, String> {
        let out_dir = env!("OUT_DIR");
        let mut optimizer_exe = PathBuf::from(out_dir);
        optimizer_exe.push("optimizer");

        let output = Command::new(optimizer_exe)
            .arg(sql)
            .output()
            .map_err(|e| e.to_string())?;

        if output.status.success() {
            let output = String::from_utf8(output.stdout).unwrap();
            if output.contains("ERROR") {
                Err(output)
            } else {
                Ok(output)
            }
        } else {
            Err(String::from_utf8(output.stderr).unwrap())
        }
    }
}

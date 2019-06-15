use std::process::Command;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, Sender};
use message::Message;
use serde_json::json;
use std::io::Write;

pub struct Optimizer;

impl Optimizer {
    pub fn new() -> Self {
        Optimizer
    }

    pub fn listen(
        &mut self,
        input_rx: Receiver<Vec<u8>>,
        transaction_tx: Sender<Vec<u8>>,
        completed_tx: Sender<Vec<u8>>
    ) {
        loop {
            let request = Message::deserialize(&input_rx.recv().unwrap()).unwrap();

            profiler::start("optimization");

            if let Message::OptimizeSQL { mut sql, connection_id } = request {
                sql.make_ascii_lowercase();

                let mut history_entry = String::new();
                history_entry.push_str(&connection_id.to_string());
                history_entry.push('\t');
                history_entry.push_str(&sql);
                history_entry.push('\n');

                let mut history_file = std::fs::OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open("sqlhistory.txt")
                    .unwrap();
                history_file.write(history_entry.as_bytes()).unwrap();

                // TODO: Add parser, optimizer support for transactions.
                // Currently, the optimizer does not support transaction keywords, so we
                // check for them manually here.
                if sql.contains("begin") {
                    transaction_tx.send(Message::BeginTransaction {
                        connection_id
                    }.serialize().unwrap()).unwrap();

                } else if sql.contains("commit") {
                    transaction_tx.send(Message::CommitTransaction {
                        connection_id
                    }.serialize().unwrap()).unwrap();

                // TODO: Add parser, optimizer support for if exists.
                // Here we just bypass the optimizer's table name resolver.
                } else if sql.contains("drop table if exists") {
                    let plan = json!({
                        "json_name": "TopLevelPlan",
                        "plan": {
                            "json_name": "DropTable",
                            "relation": "t"
                        },
                        "output_attributes": []
                    }).to_string();
                    transaction_tx.send(Message::ExecutePlan {
                        plan,
                        connection_id
                    }.serialize().unwrap()).unwrap();
                } else {
                    match self.optimize(&sql) {
                        Ok(plan) =>
                            transaction_tx.send(Message::ExecutePlan {
                                plan,
                                connection_id,
                            }.serialize().unwrap()).unwrap(),
                        Err(reason) =>
                            completed_tx.send(Message::Error {
                                reason,
                                connection_id,
                            }.serialize().unwrap()).unwrap()
                    };
                }

                profiler::end("optimization");
            } else {
                panic!("Invalid message type sent to optimizer")
            }
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

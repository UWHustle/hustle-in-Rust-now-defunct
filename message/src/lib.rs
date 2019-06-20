use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde::{Serialize, Deserialize};
use std::io::Cursor;
use types::data_type::DataType;
use std::sync::mpsc::{Receiver, Sender};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Message {
    CloseConnection { connection_id: u64 },
    ExecuteSql { sql: String },
    ParseSql { sql: String, connection_id: u64 },
    ResolveAst { ast: String, connection_id: u64 },
    OptimizePlan { plan: Plan, connection_id: u64 },
    BeginTransaction { connection_id: u64 },
    CommitTransaction { connection_id: u64 },
    ExecutePlan { plan: Plan, connection_id: u64 },
    Schema { schema: Vec<(String, DataType)>, connection_id: u64 },
    ReturnRow { row: Vec<Vec<u8>>, connection_id: u64 },
    Success { connection_id: u64 },
    Error { reason: String, connection_id: u64 },
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Plan {
    Aggregate {
        table: Box<Plan>,
        aggregates: Vec<Plan>,
        groups: Vec<Plan>
    },
    ColumnDefinition {
        name: String,
        column_type: String
    },
    ColumnReference {
        name: String,
        column_type: String,
        table: Option<String>,
        alias: Option<String>
    },
    Comparison {
        name: String,
        left: Box<Plan>,
        right: Box<Plan>
    },
    Connective {
        name: String,
        terms: Vec<Plan>
    },
    CreateTable {
        name: String,
        columns: Vec<Plan>
    },
    Delete {
        from_table: Box<Plan>,
        filter: Option<Box<Plan>>
    },
    DropTable {
        table: Box<Plan>
    },
    Function {
        name: String,
        arguments: Vec<Plan>,
        output_type: String
    },
    Insert {
        into_table: Box<Plan>,
        input: Box<Plan>
    },
    Join {
        l_table: Box<Plan>,
        r_table: Box<Plan>,
        filter: Option<Box<Plan>>
    },
    Limit {
        table: Box<Plan>,
        limit: usize
    },
    Literal {
        value: String,
        literal_type: String
    },
    Project {
        table: Box<Plan>,
        projection: Vec<Plan>
    },
    Row {
        values: Vec<Plan>
    },
    Select {
        table: Box<Plan>,
        filter: Box<Plan>
    },
    TableReference {
        name: String,
        columns: Vec<Plan>
    },
    Update {
        table: Box<Plan>,
        columns: Vec<Plan>,
        assignments: Vec<Plan>,
        filter: Option<Box<Plan>>
    }
}

impl Message {
    pub fn send<W>(&self, buf: &mut W) -> Result<(), String> where W: WriteBytesExt {
        let payload = serde_json::to_vec(self).map_err(|e| e.to_string())?;
        buf.write_u32::<BigEndian>(payload.len() as u32).map_err(|e| e.to_string())?;
        buf.write_all(&payload).map_err(|e| e.to_string())?;
        Ok(())
    }

    pub fn receive<R>(buf: &mut R) -> Result<Self, String> where R: ReadBytesExt {
        let payload_len = buf.read_u32::<BigEndian>().map_err(|e| e.to_string())?;
        let mut payload = vec![0; payload_len as usize];
        buf.read_exact(&mut payload).map_err(|e| e.to_string())?;
        serde_json::from_slice(&payload).map_err(|e| e.to_string())
    }

    pub fn serialize(&self) -> Result<Vec<u8>, String> {
        let mut buf = vec![];
        self.send(&mut buf)?;
        Ok(buf)
    }

    pub fn deserialize(buf: &[u8]) -> Result<Self, String> {
        let mut cursor = Cursor::new(buf);
        Self::receive(&mut cursor)
    }
}

pub trait Listener {
    fn listen(&mut self, input_rx: Receiver<Vec<u8>>, output_tx: Sender<Vec<u8>>);
}

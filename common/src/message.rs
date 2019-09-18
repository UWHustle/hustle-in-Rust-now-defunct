use std::io::Cursor;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};

use hustle_catalog::Column;

use crate::plan::{Plan, Statement};

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    CloseConnection,
    ExecuteSql { sql: String },
    ParseSql { sql: String },
    OptimizePlan { plan: Plan },
    TransactPlan { plan: Plan },
    ExecuteStatement { statement: Statement },
    CompleteStatement { statement: Statement },
    Schema { schema: Vec<Column> },
    ReturnRow { row: Vec<Vec<u8>> },
    Success,
    Failure { reason: String },
}

pub struct InternalMessage {
    pub connection_id: u64,
    pub inner: Message
}

impl InternalMessage {
    pub fn new(connection_id: u64, inner: Message) -> Self {
        InternalMessage {
            connection_id,
            inner,
        }
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
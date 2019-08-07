use std::io::Cursor;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};

use hustle_catalog::Column;
use crate::logical_plan::Plan;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Message {
    CloseConnection { connection_id: u64 },
    ExecuteSql { sql: String },
    ParseSql { sql: String, connection_id: u64 },
    ResolveAst { ast: String, connection_id: u64 },
    OptimizePlan { plan: Plan, connection_id: u64 },
    TransactPlan { plan: Plan, connection_id: u64 },
    ExecutePlan { plan: Plan, statement_id: u64, connection_id: u64 },
    CompletePlan { statement_id: u64, connection_id: u64 },
    Schema { schema: Vec<Column>, connection_id: u64 },
    ReturnRow { row: Vec<Vec<u8>>, connection_id: u64 },
    Success { connection_id: u64 },
    Failure { reason: String, connection_id: u64 },
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
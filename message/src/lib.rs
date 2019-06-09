use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde::{Serialize, Deserialize};
use std::io::Cursor;
use types::data_type;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Message {
    ExecuteSQL { sql: String },
    OptimizeSQL { sql: String, connection_id: u64 },
    BeginTransaction { connection_id: u64 },
    CommitTransaction { connection_id: u64 },
    ExecutePlan { plan: String, connection_id: u64 },
    Schema { schema: Vec<data_type::Variant>, connection_id: u64 },
    ReturnRow { row: Vec<Vec<u8>>, connection_id: u64 },
    Success { connection_id: u64 }
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

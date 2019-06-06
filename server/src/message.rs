use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde::{Serialize, Deserialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Message {
    Execute { statement: String }
}

impl Message {
    pub fn encode<W>(&self, buf: &mut W) -> Result<(), String> where W: WriteBytesExt {
        let payload = serde_json::to_vec(self).map_err(|e| e.to_string())?;

        let message_type = match self {
            Message::Execute { statement: _ } => 0
        };

        buf.write_u8(message_type).map_err(|e| e.to_string())?;
        buf.write_u32::<BigEndian>(payload.len() as u32).map_err(|e| e.to_string())?;
        buf.write_all(&payload).map_err(|e| e.to_string())?;
        Ok(())
    }

    pub fn decode<R>(buf: &mut R) -> Result<Self, String> where R: ReadBytesExt {
        let message_type = buf.read_u8().map_err(|e| e.to_string())?;
        let payload_len = buf.read_u32::<BigEndian>().map_err(|e| e.to_string())?;
        let mut payload = vec![0; payload_len as usize];
        buf.read_exact(&mut payload).map_err(|e| e.to_string())?;

        match message_type {
            0 => serde_json::from_slice(&payload).map_err(|e| e.to_string()),
            _ => Err("Unrecognized message type".to_string()),
        }
    }
}

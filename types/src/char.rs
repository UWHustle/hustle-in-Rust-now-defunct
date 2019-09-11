use std::str;

use crate::{CompareEq, CompareOrd, HustleType};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Char {
    len: usize,
}

impl Char {
    pub fn new(len: usize) -> Self {
        assert!(len > 0, "Cannot create Char type with zero length");
        Char {
            len,
        }
    }

    pub fn get<'a>(&self, buf: &'a [u8]) -> &'a str {
        str::from_utf8(buf).unwrap()
    }

    pub fn set(&self, val: &str, buf: &mut [u8]) {
        if val.len() < self.len {
            // Short strings are padded with the null character.
            buf[..val.len()].clone_from_slice(val.as_bytes());
            for c in &mut buf[val.len()..] {
                *c = 0x00;
            }
        } else {
            // Long strings are truncated.
            buf.clone_from_slice(val[..self.len].as_bytes());
        }
    }

    pub fn new_buf(&self, val: &str) -> Vec<u8> {
        let mut buf = vec![0; self.len];
        self.set(val, &mut buf);
        buf
    }
}

impl HustleType for Char {
    fn byte_len(&self) -> usize {
        self.len
    }

    fn to_string(&self, buf: &[u8]) -> String {
        self.get(buf).to_owned()
    }
}

impl CompareEq<Char> for Char {
    fn compare_eq(&self, _other: &Char, left: &[u8], right: &[u8]) -> bool {
        left == right
    }
}

impl CompareOrd<Char> for Char {
    fn compare_lt(&self, _other: &Char, left: &[u8], right: &[u8]) -> bool {
        left < right
    }

    fn compare_le(&self, _other: &Char, left: &[u8], right: &[u8]) -> bool {
        left <= right
    }

    fn compare_gt(&self, _other: &Char, left: &[u8], right: &[u8]) -> bool {
        left > right
    }

    fn compare_ge(&self, _other: &Char, left: &[u8], right: &[u8]) -> bool {
        left >= right
    }
}

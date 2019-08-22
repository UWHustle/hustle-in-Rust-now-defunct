use std::str;

use crate::{CompareEq, CompareOrd, HustleType};

#[derive(Serialize, Deserialize)]
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
}

impl HustleType for Char {
    fn byte_len(&self) -> usize {
        self.len
    }
}

impl CompareEq<Char> for Char {
    fn eq(&self, _other: &Char, left: &[u8], right: &[u8]) -> bool {
        left == right
    }
}

impl CompareOrd<Char> for Char {
    fn lt(&self, _other: &Char, left: &[u8], right: &[u8]) -> bool {
        left < right
    }

    fn le(&self, _other: &Char, left: &[u8], right: &[u8]) -> bool {
        left <= right
    }

    fn gt(&self, _other: &Char, left: &[u8], right: &[u8]) -> bool {
        left > right
    }

    fn ge(&self, _other: &Char, left: &[u8], right: &[u8]) -> bool {
        left >= right
    }
}

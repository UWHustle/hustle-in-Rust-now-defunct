#[macro_use]
extern crate serde;

use crate::bits::Bits;
use crate::char::Char;
use crate::primitive::{
    Bool,
    Int16,
    Int32,
    Int64,
    Int8,
    UInt16,
    UInt32,
    UInt64,
    UInt8,
};

pub mod bits;
pub mod char;
pub mod primitive;

#[derive(Serialize, Deserialize)]
pub enum TypeVariant {
    Bool(Bool),
    Int8(Int8),
    Int16(Int16),
    Int32(Int32),
    Int64(Int64),
    UInt8(UInt8),
    UInt16(UInt16),
    UInt32(UInt32),
    UInt64(UInt64),
    Char(Char),
    Bits(Bits),
}

pub trait HustleType {
    fn byte_len(&self) -> usize;
}

pub trait CompareEq<T> {
    fn eq(&self, other: &T, left: &[u8], right: &[u8]) -> bool;
}

pub trait CompareOrd<T> {
    fn lt(&self, other: &T, left: &[u8], right: &[u8]) -> bool;
    fn le(&self, other: &T, left: &[u8], right: &[u8]) -> bool;
    fn gt(&self, other: &T, left: &[u8], right: &[u8]) -> bool;
    fn ge(&self, other: &T, left: &[u8], right: &[u8]) -> bool;
}

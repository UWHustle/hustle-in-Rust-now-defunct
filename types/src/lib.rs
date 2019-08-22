#[macro_use]
extern crate serde;

pub use crate::bits::Bits;
pub use crate::char::Char;
pub use crate::primitive::{
    Bool,
    Int16,
    Int32,
    Int64,
    Int8,
};

pub mod bits;
pub mod char;
pub mod primitive;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TypeVariant {
    Bool(Bool),
    Int8(Int8),
    Int16(Int16),
    Int32(Int32),
    Int64(Int64),
    Char(Char),
    Bits(Bits),
}

impl TypeVariant {
    pub fn byte_len(&self) -> usize {
        match self {
            TypeVariant::Bool(t) => t.byte_len(),
            TypeVariant::Int8(t) => t.byte_len(),
            TypeVariant::Int16(t) => t.byte_len(),
            TypeVariant::Int32(t) => t.byte_len(),
            TypeVariant::Int64(t) => t.byte_len(),
            TypeVariant::Char(t) => t.byte_len(),
            TypeVariant::Bits(t) => t.byte_len(),
        }
    }
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

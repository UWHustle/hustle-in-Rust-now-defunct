#[macro_use]
extern crate serde;

pub mod int_16;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum HustleTypeVariant {
    Int8,
    Int16,
}

pub trait HustleType {
    fn variant(&self) -> HustleTypeVariant;
    fn size(&self) -> usize;
}

trait PrimitiveType {
    type Primitive;
    fn as_primitive(&self) -> Self::Primitive;
}

pub trait Comparable<T> {
    fn eq(&self, other: &T) -> bool;
    fn lt(&self, other: &T) -> bool;
}

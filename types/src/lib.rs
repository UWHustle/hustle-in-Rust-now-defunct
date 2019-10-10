#[macro_use]
extern crate serde;

#[macro_use]
extern crate downcast_rs;

pub use crate::bits::Bits;
pub use crate::char::Char;
pub use crate::primitive::{
    Bool,
    Int16,
    Int32,
    Int64,
    Int8,
};
use downcast_rs::Downcast;

pub mod bits;
pub mod char;
pub mod primitive;

/// An enum wrapper around each struct the implements `HustleType` to allow matching, serialization,
/// and deserialization.
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
    pub fn into_type(self) -> Box<dyn HustleType> {
        match self {
            TypeVariant::Bool(t) => Box::new(t),
            TypeVariant::Int8(t) => Box::new(t),
            TypeVariant::Int16(t) => Box::new(t),
            TypeVariant::Int32(t) => Box::new(t),
            TypeVariant::Int64(t) => Box::new(t),
            TypeVariant::Char(t) => Box::new(t),
            TypeVariant::Bits(t) => Box::new(t),
        }
    }
}

/// The main trait that all types must implement.
pub trait HustleType: Downcast {
    fn byte_len(&self) -> usize;
    fn to_string(&self, buf: &[u8]) -> String;
}

impl_downcast!(HustleType);

/// A trait that allows a type to specify whether it is equal to another.
pub trait CompareEq<T> {
    fn compare_eq(&self, other: &T, left: &[u8], right: &[u8]) -> bool;
}

/// A trait the allows a type to specify whether it is ordered with respect to another.
pub trait CompareOrd<T> {
    fn compare_lt(&self, other: &T, left: &[u8], right: &[u8]) -> bool;
    fn compare_le(&self, other: &T, left: &[u8], right: &[u8]) -> bool;
    fn compare_gt(&self, other: &T, left: &[u8], right: &[u8]) -> bool;
    fn compare_ge(&self, other: &T, left: &[u8], right: &[u8]) -> bool;
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum ComparativeVariant {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

/// A convenience function that matches two `TypeVariant`s and compares their buffers via the
/// specified `comparative_variant`. An `Err` is returned if the types are not comparable.
pub fn compare(
    comparative_variant: ComparativeVariant,
    l_type_variant: &TypeVariant,
    r_type_variant: &TypeVariant,
    l_buf: &[u8],
    r_buf: &[u8],
) -> Result<bool, String> {
    match (l_type_variant, r_type_variant) {
        (TypeVariant::Bool(l_type), TypeVariant::Bool(r_type)) => {
            compare_eq_ord(comparative_variant, l_type, r_type, l_buf, r_buf)
        },
        (TypeVariant::Int8(l_type), TypeVariant::Int8(r_type)) => {
            compare_eq_ord(comparative_variant, l_type, r_type, l_buf, r_buf)
        },
        (TypeVariant::Int8(l_type), TypeVariant::Int16(r_type)) => {
            compare_eq_ord(comparative_variant, l_type, r_type, l_buf, r_buf)
        },
        (TypeVariant::Int8(l_type), TypeVariant::Int32(r_type)) => {
            compare_eq_ord(comparative_variant, l_type, r_type, l_buf, r_buf)
        },
        (TypeVariant::Int8(l_type), TypeVariant::Int64(r_type)) => {
            compare_eq_ord(comparative_variant, l_type, r_type, l_buf, r_buf)
        },
        (TypeVariant::Int16(l_type), TypeVariant::Int8(r_type)) => {
            compare_eq_ord(comparative_variant, l_type, r_type, l_buf, r_buf)
        },
        (TypeVariant::Int16(l_type), TypeVariant::Int16(r_type)) => {
            compare_eq_ord(comparative_variant, l_type, r_type, l_buf, r_buf)
        },
        (TypeVariant::Int16(l_type), TypeVariant::Int32(r_type)) => {
            compare_eq_ord(comparative_variant, l_type, r_type, l_buf, r_buf)
        },
        (TypeVariant::Int16(l_type), TypeVariant::Int64(r_type)) => {
            compare_eq_ord(comparative_variant, l_type, r_type, l_buf, r_buf)
        },
        (TypeVariant::Int32(l_type), TypeVariant::Int8(r_type)) => {
            compare_eq_ord(comparative_variant, l_type, r_type, l_buf, r_buf)
        },
        (TypeVariant::Int32(l_type), TypeVariant::Int16(r_type)) => {
            compare_eq_ord(comparative_variant, l_type, r_type, l_buf, r_buf)
        },
        (TypeVariant::Int32(l_type), TypeVariant::Int32(r_type)) => {
            compare_eq_ord(comparative_variant, l_type, r_type, l_buf, r_buf)
        },
        (TypeVariant::Int32(l_type), TypeVariant::Int64(r_type)) => {
            compare_eq_ord(comparative_variant, l_type, r_type, l_buf, r_buf)
        },
        (TypeVariant::Int64(l_type), TypeVariant::Int8(r_type)) => {
            compare_eq_ord(comparative_variant, l_type, r_type, l_buf, r_buf)
        },
        (TypeVariant::Int64(l_type), TypeVariant::Int16(r_type)) => {
            compare_eq_ord(comparative_variant, l_type, r_type, l_buf, r_buf)
        },
        (TypeVariant::Int64(l_type), TypeVariant::Int32(r_type)) => {
            compare_eq_ord(comparative_variant, l_type, r_type, l_buf, r_buf)
        },
        (TypeVariant::Int64(l_type), TypeVariant::Int64(r_type)) => {
            compare_eq_ord(comparative_variant, l_type, r_type, l_buf, r_buf)
        },
        (TypeVariant::Char(l_type), TypeVariant::Char(r_type)) => {
            compare_eq_ord(comparative_variant, l_type, r_type, l_buf, r_buf)
        },
        (TypeVariant::Bits(l_type), TypeVariant::Bits(r_type)) => {
            match comparative_variant {
                ComparativeVariant::Eq => Ok(l_type.compare_eq(r_type, l_buf, r_buf)),
                _ => Err(comparison_error(comparative_variant, l_type_variant, r_type_variant)),
            }
        }
        _ => Err(comparison_error(comparative_variant, l_type_variant, r_type_variant)),
    }
}

fn compare_eq_ord<L, R>(
    comparative_variant: ComparativeVariant,
    l_type: &L,
    r_type: &R,
    l_buf: &[u8],
    r_buf: &[u8],
) -> Result<bool, String>
where
    L: CompareEq<R> + CompareOrd<R>,
{
    match comparative_variant {
        ComparativeVariant::Eq => Ok(l_type.compare_eq(&r_type, l_buf, r_buf)),
        ComparativeVariant::Lt => Ok(l_type.compare_lt(&r_type, l_buf, r_buf)),
        ComparativeVariant::Le => Ok(l_type.compare_le(&r_type, l_buf, r_buf)),
        ComparativeVariant::Gt => Ok(l_type.compare_gt(&r_type, l_buf, r_buf)),
        ComparativeVariant::Ge => Ok(l_type.compare_ge(&r_type, l_buf, r_buf)),
    }
}

fn comparison_error(
    comparative_variant: ComparativeVariant,
    l_type_variant: &TypeVariant,
    r_type_variant: &TypeVariant,
) -> String {
    format!(
        "Cannot perform comparison \"{:?}\" between {:?} and {:?}",
        comparative_variant,
        l_type_variant,
        r_type_variant,
    )
}

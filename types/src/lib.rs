#[macro_use]
extern crate serde;

use serde::export::PhantomData;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataTypeVariant {
    Int8,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataType {
    variant: DataTypeVariant,
    size: usize,
    nullable: bool,
}

impl DataType {
    fn new(variant: DataTypeVariant, size: usize, nullable: bool) -> Self {
        DataType {
            variant,
            size,
            nullable,
        }
    }

    pub fn int_8(nullable: bool) -> Self {
        Self::new(DataTypeVariant::Int8, 1, nullable)
    }

    pub fn variant(&self) -> &DataTypeVariant {
        &self.variant
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }
}

pub trait Value<'a> {
    fn bind(buf: &'a [u8]) -> Self;
}

pub trait Comparable<T> {
    fn eq(&self, other: &T) -> bool;
    fn lt(&self, other: &T) -> bool;
}

pub struct Int8<'a> {
    buf: &'a [u8]
}

impl<'a> Value<'a> for Int8<'a> {
    fn bind(buf: &[u8]) -> Int8 {
        Int8 {
            buf
        }
    }
}

impl<'a> Comparable<Int8<'a>> for Int8<'a> {
    fn eq(&self, other: &Int8<'a>) -> bool {
        self.buf == other.buf
    }

    fn lt(&self, other: &Int8<'a>) -> bool {
        unimplemented!()
    }
}

pub struct Comparison<O, L, R> {
    left: L,
    right: R,
    operator: PhantomData<O>,
}

pub struct EqOperator;

impl<L, R> Comparison<EqOperator, L, R>
where
    L: Comparable<R>,
{
    pub fn compare(&self) {
        self.left.eq(&self.right);
    }
}

#[test]
fn test() {
    let left_buf = vec![1];
    let right_buf = vec![1];

    let left = Int8::bind(&left_buf);
    let right = Int8::bind(&right_buf);

    assert!(left.eq(&right));
}

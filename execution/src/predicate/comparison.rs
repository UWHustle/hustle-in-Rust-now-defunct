use std::marker::PhantomData;

use bit_vec::BitVec;

use hustle_storage::block::BlockReference;
use hustle_types::{Comparable, HustleType};

use crate::predicate::Predicate;

struct EqValueComparison;
struct LtValueComparison;

pub struct ValueComparisonPredicate<C, L, R> {
    col: usize,
    right: R,
    comparison: C,
    phantom: PhantomData<L>,
}

impl<C, L, R> ValueComparisonPredicate<C, L, R>
where
    L: for<'a> HustleType<&'a [u8]> + Comparable<R>,
{
    fn new(col: usize, right: R, comparison: C) -> Self {
        ValueComparisonPredicate {
            col,
            right,
            comparison,
            phantom: PhantomData,
        }
    }
}

impl<C, L, R> Predicate for ValueComparisonPredicate<C, L, R>
where
    L: for <'a> HustleType<&'a [u8]> + Comparable<R>
{
    fn evaluate(&self, block: &BlockReference) -> BitVec {
        let mut bits = BitVec::from_elem(block.get_n_rows(), false);
        for (i, buf) in block.get_col(self.col).enumerate() {
            bits.set(i, L::interpret(buf).eq(&self.right));
        }
        bits
    }
}

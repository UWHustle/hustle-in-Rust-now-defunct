use std::marker::PhantomData;

use bit_vec::BitVec;

use hustle_storage::block::BlockReference;
use hustle_types::{Comparable, Value};

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
    L: for<'a> Value<'a> + Comparable<R>
{
    fn new(col: usize, right: R, comparison: C) -> Self {
        ValueComparisonPredicate {
            col,
            right,
            comparison,
            phantom: PhantomData,
        }
    }

    fn evaluate_with_comparison<F>(&self, block: &BlockReference, comparison: F) -> BitVec
    where
        F: Fn(L, &R) -> bool,
    {
        block.mask_from_predicate(self.col, |v| comparison(L::bind(v), &self.right))
    }
}

impl<L, R> ValueComparisonPredicate<EqValueComparison, L, R>
where
    L: for<'a> Value<'a> + Comparable<R>,
{
    pub fn eq(col: usize, right: R) -> Self {
        Self::new(col, right, EqValueComparison)
    }
}

impl<L, R> Predicate for ValueComparisonPredicate<EqValueComparison, L, R>
where
    L: for<'a> Value<'a> + Comparable<R>,
{
    fn evaluate(&self, block: &BlockReference) -> BitVec {
        self.evaluate_with_comparison(block, |l, r| l.eq(r))
    }
}

impl<L, R> ValueComparisonPredicate<LtValueComparison, L, R>
where
    L: for<'a> Value<'a> + Comparable<R>,
{
    pub fn lt(col: usize, right: R) -> Self {
        Self::new(col, right, LtValueComparison)
    }
}

impl<L, R> Predicate for ValueComparisonPredicate<LtValueComparison, L, R>
where
    L: for<'a> Value<'a> + Comparable<R>,
{
    fn evaluate(&self, block: &BlockReference) -> BitVec {
        self.evaluate_with_comparison(block, |l, r| l.lt(r))
    }
}

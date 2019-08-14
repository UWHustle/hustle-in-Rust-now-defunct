use bit_vec::BitVec;

use hustle_storage::block::BlockReference;

mod comparison;
mod conjunction;
mod disjunction;

pub trait Predicate {
    fn evaluate(&self, block: &BlockReference) -> BitVec;
}

use crate::predicate::Predicate;
use hustle_storage::block::BlockReference;
use bit_vec::BitVec;

pub struct DisjunctionPredicate {
    terms: Vec<Box<dyn Predicate>>,
}

impl Predicate for DisjunctionPredicate {
    fn evaluate(&self, block: &BlockReference) -> BitVec {
        let mut matches = block.mask_from_value(false);
        for result in self.terms.iter().map(|term| term.evaluate(block)) {
            matches.union(&result);
        }
        matches
    }
}

use crate::predicate::Predicate;
use hustle_storage::block::BlockReference;
use bit_vec::BitVec;

pub struct ConjunctionPredicate {
    terms: Vec<Box<dyn Predicate>>,
}

impl Predicate for ConjunctionPredicate {
    fn evaluate(&self, block: &BlockReference) -> BitVec {
        let mut matches = block.mask_from_value(true);
        for result in self.terms.iter().map(|term| term.evaluate(block)) {
            matches.intersect(&result);
        }
        matches
    }
}

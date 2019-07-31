pub mod eliminate_join;
pub mod fuse_operators;
pub mod rewrite_predicate;
pub mod set_evaluation_order;

use hustle_common::{Database, PhysicalPlan};

pub trait Rule {
    fn apply(&self, database: &Database, physical_plan: &mut PhysicalPlan);

    fn name(&self) -> &'static str;
}

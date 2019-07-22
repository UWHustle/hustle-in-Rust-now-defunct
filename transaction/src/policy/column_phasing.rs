use crate::policy::{Policy, PolicyHelper};
use hustle_common::Plan;

pub struct ColumnPhasingPolicy {
    policy_helper: PolicyHelper
}

impl ColumnPhasingPolicy {
    pub fn new() -> Self {
        ColumnPhasingPolicy {
            policy_helper: PolicyHelper::new(),
        }
    }
}

impl Policy for ColumnPhasingPolicy {
    fn begin_transaction(&mut self) -> u64 {
        unimplemented!()
    }

    fn commit_transaction(&mut self, transaction_id: u64, callback: &Fn(Plan, u64)) {
        unimplemented!()
    }

    fn enqueue_statement(&mut self, transaction_id: u64, plan: Plan, callback: &Fn(Plan, u64)) {
        unimplemented!()
    }

    fn complete_statement(&mut self, statement_id: u64, callback: &Fn(Plan, u64)) {
        unimplemented!()
    }
}

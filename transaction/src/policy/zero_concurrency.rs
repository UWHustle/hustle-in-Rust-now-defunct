use std::collections::VecDeque;
use crate::policy::Policy;
use message::Plan;

pub struct ZeroConcurrencyPolicy {
    waiting_statements: VecDeque<Plan>,
    running_statement: bool,
}

impl ZeroConcurrencyPolicy {
    pub fn new() -> Self {
        ZeroConcurrencyPolicy {
            waiting_statements: VecDeque::new(),
            running_statement: false,
        }
    }
}

impl Policy for ZeroConcurrencyPolicy {
    fn new_statement(&mut self, statement: Plan) -> Vec<Plan> {
        if self.running_statement {
            self.waiting_statements.push_back(statement);
            vec![]
        } else {
            self.running_statement = true;
            return vec![statement]
        }
    }

    fn complete_statement(&mut self, statement: Plan) -> Vec<Plan> {
        self.running_statement = false;
        match self.waiting_statements.pop_front() {
            Some(s) => {
                self.running_statement = true;
                vec![s]
            },
            None => vec![]
        }
    }
}

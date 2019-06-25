use std::collections::VecDeque;
use message::Plan;

pub struct Transaction {
    plans: VecDeque<Plan>,
    pub committed: bool
}

impl Transaction {
    pub fn new() -> Self {
        Transaction {
            plans: VecDeque::new(),
            committed: false
        }
    }

    pub fn enqueue_plan(&mut self, statement: Plan) {
        self.plans.push_back(statement);
    }

    pub fn dequeue_plan(&mut self) -> Option<Plan> {
        self.plans.pop_front()
    }
}

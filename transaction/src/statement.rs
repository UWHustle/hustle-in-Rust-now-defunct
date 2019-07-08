use message::Plan;

pub struct Statement {
    pub id: u64,
    pub plan: Plan,
}

impl Statement {
    pub fn new(id: u64, plan: Plan) -> Self {
        Statement {
            id,
            plan,
        }
    }
}

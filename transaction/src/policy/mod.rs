pub use zero_concurrency::ZeroConcurrencyPolicy;
use message::Plan;

mod zero_concurrency;

trait Policy {
    fn new_statement(&mut self, statement: Plan) -> Vec<Plan>;
    fn complete_statement(&mut self, statement: Plan) -> Vec<Plan>;
}



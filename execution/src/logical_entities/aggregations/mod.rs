use type_system::type_id::*;
use type_system::*;

pub mod avg;
pub mod count;
pub mod max;
pub mod min;
pub mod sum;

pub trait AggregationTrait: BoxCloneAggregation {
    fn get_name(&self) -> &'static str;
    fn initialize(&mut self) -> ();
    fn consider_value(&mut self, value: &Value) -> ();
    fn output(&self) -> Box<Value>;
    fn output_type(&self) -> TypeID;
}

pub trait BoxCloneAggregation {
    fn box_clone_aggregation(&self) -> Box<AggregationTrait>;
}

impl<T: AggregationTrait + Clone + 'static> BoxCloneAggregation for T {
    fn box_clone_aggregation(&self) -> Box<AggregationTrait> {
        Box::new(self.clone())
    }
}

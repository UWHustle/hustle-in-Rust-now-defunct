use type_system::type_id::*;
use type_system::*;

pub mod avg;
pub mod count;
pub mod max;
pub mod min;
pub mod sum;

pub trait AggregationTrait {
    fn get_name(&self) -> &'static str;
    fn initialize(&mut self) -> ();
    fn consider_value(&mut self, value: &Value) -> ();
    fn output(&self) -> Box<Value>;
    fn output_type(&self) -> TypeID;
}

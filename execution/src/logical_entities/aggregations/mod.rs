use logical_entities::types::ValueType;
use logical_entities::types::TypeID;

pub mod avg;
pub mod count;
pub mod max;
pub mod min;
pub mod sum;

pub trait AggregationTrait {
    fn get_name(&self) -> &'static str;
    fn initialize(&mut self) -> ();
    fn consider_value(&mut self, value: &ValueType) -> ();
    fn output(&self) -> Box<ValueType>;
    fn output_type(&self) -> TypeID;
}
pub mod count;
pub mod sum;

pub trait AggregationTrait {
    fn get_name(&self) -> &'static str;
    fn initialize(&mut self) -> ();
    fn consider_value(&mut self, value: Vec<u8>) -> (); //, Column) -> ();
    fn output(&self) -> (Vec<u8>);
}
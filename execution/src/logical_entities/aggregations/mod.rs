pub mod count;
pub mod sum;

use logical_entities::schema::Schema;
use logical_entities::column::Column;
use logical_entities::relation::Relation;

pub trait AggregationTrait {
    fn output_schema(&self) -> Schema;
    fn input_relation(&self) -> Relation;
    fn group_by_columns(&self) -> Vec<Column>;
    fn initialize(&mut self) -> ();
    fn consider_value(&mut self, Vec<u8>, Column) -> ();
    fn output(&self) -> (Vec<u8>);
}
use logical_entities::aggregations::AggregationTrait;
use type_system::ValueType;
use type_system::type_id::*;
use type_system::integer::*;
use type_system::*;
use type_system::type_id::*;

#[derive(Clone, Debug, PartialEq)]
pub struct Count {
    running_total: u32
}

impl Count {
    pub fn new() -> Self {
        Count { running_total: 0 }
    }
}

impl AggregationTrait for Count {
    fn get_name(&self) -> &'static str {
        "COUNT"
    }

    fn initialize(&mut self) -> () {
        self.running_total = 0;
    }

    #[allow(unused_variables)]
    fn consider_value(&mut self, value: &ValueType) -> () {
        self.running_total += 1;
    }

    fn output(&self) -> Box<ValueType> {
        Int8::new(self.running_total as i64).box_clone()
    }

    fn output_type(&self) -> TypeID {
        TypeID::Int4(false)
    }
}

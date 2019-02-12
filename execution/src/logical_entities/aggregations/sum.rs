use logical_entities::aggregations::AggregationTrait;
use type_system::type_id::*;
use type_system::Numeric;
use type_system::ValueType;
use type_system::force_numeric;

#[derive(Clone, Debug)]
pub struct Sum {
    running_total: Box<Numeric>,
}

impl Sum {
    pub fn new(type_id: TypeID) -> Self {
        Sum { running_total: type_id.create_zero() }
    }
}

impl AggregationTrait for Sum {
    fn get_name(&self) -> &'static str {
        "SUM"
    }

    fn initialize(&mut self) -> () {
        self.running_total = self.output_type().create_zero();
    }

    fn consider_value(&mut self, value: &ValueType) -> () {
        self.running_total = self.running_total.add(force_numeric(value));
    }

    fn output(&self) -> Box<ValueType> {
        self.running_total.box_clone()
    }

    fn output_type(&self) -> TypeID {
        self.running_total.type_id().clone()
    }
}

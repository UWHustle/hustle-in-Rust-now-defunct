use logical_entities::aggregations::AggregationTrait;
use logical_entities::types::TypeID;
use logical_entities::types::Numeric;
use logical_entities::types::ValueType;
use logical_entities::types::BoxedNumeric;
use logical_entities::types::*;
use logical_entities::types::force_numeric;

#[derive(Clone)]
pub struct Sum {
    running_total: BoxedNumeric,
}

impl Sum {
    pub fn new(type_id: TypeID) -> Self {
        Sum { running_total: BoxedNumeric::new(type_id.create_zero().box_numeric_clone()) }
    }
}

impl AggregationTrait for Sum {
    fn get_name(&self) -> &'static str {
        "SUM"
    }

    fn initialize(&mut self) -> () {
        self.running_total = BoxedNumeric::new(self.output_type().create_zero());
    }

    fn consider_value(&mut self, value: &ValueType) -> () {
        self.running_total = BoxedNumeric::new(self.running_total.value().add(force_numeric(value)));
    }

    fn output(&self) -> &ValueType {
        self.running_total.value().as_value_type()
    }

    fn output_type(&self) -> TypeID {
        self.running_total.value().type_id().clone()
    }
}

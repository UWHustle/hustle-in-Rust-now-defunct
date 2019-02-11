use logical_entities::aggregations::AggregationTrait;
use logical_entities::types::TypeID;
use logical_entities::types::*;

#[derive(Clone, Debug)]
pub struct Min {
    current_min: BoxedValue
}

impl Min {
    pub fn new(data_type: TypeID) -> Self {
        Min {
            current_min: BoxedValue::new(data_type.create_null()),
        }
    }
}

impl AggregationTrait for Min {
    fn get_name(&self) -> &'static str {
        "MIN"
    }

    fn initialize(&mut self) -> () {
        self.current_min = BoxedValue::new(self.output_type().create_null());
    }

    fn consider_value(&mut self, value: &ValueType) -> () {
        if self.current_min.value().is_null() || self.current_min.value().greater(value) {
            self.current_min = BoxedValue::new(value.box_clone());
        }
    }

    fn output(&self) -> &ValueType {
        &*self.current_min.value()
    }

    fn output_type(&self) -> TypeID {
        self.current_min.value().type_id().clone()
    }
}

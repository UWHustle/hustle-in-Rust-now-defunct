use logical_entities::aggregations::AggregationTrait;
use type_system::type_id::*;
use type_system::*;

#[derive(Clone, Debug)]
pub struct Min {
    current_min: Box<Value>,
}

impl Min {
    pub fn new(data_type: TypeID) -> Self {
        Min {
            current_min: data_type.create_null(),
        }
    }
}

impl AggregationTrait for Min {
    fn get_name(&self) -> &'static str {
        "MIN"
    }

    fn initialize(&mut self) {
        self.current_min = self.output_type().create_null();
    }

    fn consider_value(&mut self, value: &Value) {
        if self.current_min.is_null() || self.current_min.greater(value) {
            self.current_min = value.box_clone_value();
        }
    }

    fn output(&self) -> Box<Value> {
        self.current_min.box_clone_value()
    }

    fn output_type(&self) -> TypeID {
        self.current_min.type_id().clone()
    }
}

use logical_entities::aggregations::AggregationTrait;
use type_system::data_type::*;
use type_system::*;

#[derive(Clone, Debug)]
pub struct Max {
    current_max: Box<Value>,
}

impl Max {
    pub fn new(data_type: DataType) -> Self {
        Max {
            current_max: data_type.create_null(),
        }
    }
}

impl AggregationTrait for Max {
    fn get_name(&self) -> &'static str {
        "MAX"
    }

    fn initialize(&mut self) {
        self.current_max = self.output_type().create_null();
    }

    fn consider_value(&mut self, value: &Value) {
        if self.current_max.is_null() || !self.current_max.greater(value) {
            self.current_max = value.box_clone_value();
        }
    }

    fn output(&self) -> Box<Value> {
        self.current_max.box_clone_value()
    }

    fn output_type(&self) -> DataType {
        self.current_max.data_type().clone()
    }
}

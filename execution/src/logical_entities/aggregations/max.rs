use logical_entities::aggregations::AggregationTrait;
use logical_entities::types::TypeID;
use logical_entities::types::ValueType;

pub struct Max {
    current_max: Box<ValueType>,
}

impl Max {
    pub fn new(data_type: TypeID) -> Self {
        Max { current_max: data_type.create_null() }
    }
}

impl AggregationTrait for Max {
    fn get_name(&self) -> &'static str {
        "MAX"
    }

    fn initialize(&mut self) -> () {
        self.current_max = self.output_type().create_null();
    }

    fn consider_value(&mut self, value: &ValueType) -> () {
        if self.current_max.is_null() || !self.current_max.greater(value) {
            self.current_max = value.box_clone();
        }
    }

    fn output(&self) -> &ValueType {
        &*self.current_max
    }

    fn output_type(&self) -> TypeID {
        self.current_max.type_id().clone()
    }
}

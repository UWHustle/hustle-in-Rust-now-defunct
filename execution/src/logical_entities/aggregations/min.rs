use logical_entities::aggregations::AggregationTrait;
use logical_entities::types::TypeID;
use logical_entities::types::ValueType;

pub struct Min {
    current_min: Box<ValueType>
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

    fn initialize(&mut self) -> () {
        self.current_min = self.output_type().create_null();
    }

    fn consider_value(&mut self, value: &ValueType) -> () {
        if self.current_min.is_null() || self.current_min.greater(value) {
            self.current_min = value.box_clone();
        }
    }

    fn output(&self) -> (Box<ValueType>) {
        self.current_min.box_clone()
    }

    fn output_type(&self) -> TypeID {
        self.current_min.type_id().clone()
    }
}

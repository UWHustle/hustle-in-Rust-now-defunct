use logical_entities::aggregations::AggregationTrait;
use logical_entities::types::TypeID;
use logical_entities::types::ValueType;

#[derive(Clone, Debug, PartialEq)]
pub struct Max {
    data_type: TypeID,
    current_max: Box<ValueType>,
}

impl Max {
    pub fn new(data_type: TypeID) -> Self {
        Max {
            data_type,
            current_max : TypeID::create_null(),
        }
    }
}

impl AggregationTrait for Max {
    fn get_name(&self) -> &'static str {
        "MAX"
    }

    fn initialize(&mut self) -> () {
        self.current_max = TypeID::create_null();
    }

    fn consider_value(&mut self, value: Vec<u8>) -> () {
        if self.current_max.is_null() || !self.current_max.greater(&value) {
            self.current_max = value;
        }
    }

    fn output(&self) -> (Box<ValueType>) {
        self.current_max.clone()
    }

    fn output_type(&self) -> TypeID {
        self.data_type.clone()
    }
}

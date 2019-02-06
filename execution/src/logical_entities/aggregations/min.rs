use logical_entities::aggregations::AggregationTrait;
use logical_entities::types::TypeID;
use logical_entities::types::ValueType;

#[derive(Clone, Debug, PartialEq)]
pub struct Min {
    data_type: TypeID,
    current_min: Box<ValueType>,
}

impl Min {
    pub fn new(data_type: TypeID) -> Self {
        Min {
            data_type,
            current_min: TypeID::create_null(),
        }
    }
}

impl AggregationTrait for Min {
    fn get_name(&self) -> &'static str {
        "MIN"
    }

    fn initialize(&mut self) -> () {
        self.current_min = TypeID::create_null();
    }

    fn consider_value(&mut self, value: Vec<u8>) -> () {
        if self.current_min.is_null() || self.current_min.greater(&value) {
            self.current_min = value;
        }
    }

    fn output(&self) -> (Box<ValueType>) {
        self.current_min.clone()
    }

    fn output_type(&self) -> TypeID {
        self.data_type.clone()
    }
}

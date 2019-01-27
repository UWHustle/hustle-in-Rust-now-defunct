use logical_entities::aggregations::AggregationTrait;
use logical_entities::types::DataType;

#[derive(Clone, Debug, PartialEq)]
pub struct Max {
    data_type: DataType,
    current_max: Vec<u8>,
}

impl Max {
    pub fn new(data_type: DataType) -> Self {
        Max {
            data_type,
            current_max: vec!(),
        }
    }
}

impl AggregationTrait for Max {
    fn get_name(&self) -> &'static str {
        "MAX"
    }

    fn initialize(&mut self) -> () {
        self.current_max = vec!();
    }

    fn consider_value(&mut self, value: Vec<u8>) -> () {
        if self.current_max.is_empty() || self.data_type.compare(&self.current_max, &value) < 0 {
            self.current_max = value;
        }
    }

    fn output(&self) -> (Vec<u8>) {
        self.current_max.clone()
    }

    fn output_type(&self) -> DataType {
        self.data_type.clone()
    }
}
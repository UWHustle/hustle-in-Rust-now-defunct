use logical_entities::aggregations::AggregationTrait;
use logical_entities::types::DataType;
use logical_entities::types::integer::IntegerType;

#[derive(Clone, Debug, PartialEq)]
pub struct Min {
    data_type: DataType,
    current_min: Vec<u8>
}

impl Min {
    pub fn new(data_type: DataType) -> Self {
        Min {
            data_type,
            current_min: vec!()
        }
    }
}

impl AggregationTrait for Min {
    fn get_name(&self) -> &'static str {
        "MIN"
    }

    fn initialize(&mut self) -> () {
        self.current_min = vec!();
    }

    fn consider_value(&mut self, value: Vec<u8>) -> () {
        if self.current_min.is_empty() {
            self.current_min = value;
        } else {
            let comp = self.data_type.compare(&self.current_min, &value);
            if comp > 0 {
                self.current_min = value;
            }
        }
    }

    fn output(&self) -> (Vec<u8>) {
        self.current_min.clone()
    }
}

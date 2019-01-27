use logical_entities::aggregations::AggregationTrait;
use logical_entities::types::DataType;

#[derive(Clone, Debug, PartialEq)]
pub struct Sum {
    data_type: DataType,
    running_total: Vec<u8>
}

impl Sum {
    pub fn new(data_type: DataType) -> Self {
        Sum {
            data_type,
            running_total: vec!()
        }
    }
}

impl AggregationTrait for Sum {

    fn get_name(&self) -> &'static str {
        "SUM"
    }

    fn initialize(&mut self) -> () {
        self.running_total = vec!();
    }

    fn consider_value(&mut self, value: Vec<u8>) -> () {
        self.running_total = self.data_type.sum(&self.running_total, &value).0;
    }

    fn output(&self) -> (Vec<u8>) {
        self.running_total.clone()
    }
}

use logical_entities::aggregations::AggregationTrait;
use logical_entities::types::DataType;
use logical_entities::types::DataTypeTrait;
use logical_entities::types::integer::IntegerType;

#[derive(Clone, Debug, PartialEq)]
pub struct Count {
    running_total: u8
}

impl Count {
    pub fn new() -> Self {
        Count { running_total: 0 }
    }
}

impl AggregationTrait for Count {
    fn get_name(&self) -> &'static str {
        "COUNT"
    }

    fn initialize(&mut self) -> () {
        self.running_total = 0;
    }

    #[allow(unused_variables)]
    fn consider_value(&mut self, value: Vec<u8>) -> () {
        self.running_total += 1;
    }

    fn output(&self) -> (Vec<u8>) {
        let output = self.running_total.to_string();
        let (output_bits, _size) = IntegerType::parse_and_marshall(output);
        output_bits
    }

    fn output_type(&self) -> DataType {
        DataType::Integer
    }
}

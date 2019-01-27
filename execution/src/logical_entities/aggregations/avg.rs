use logical_entities::aggregations::AggregationTrait;
use logical_entities::types::DataType;
use logical_entities::types::integer::IntegerType;

#[derive(Clone, Debug, PartialEq)]
pub struct Avg {
    data_type: DataType,
    sum: Vec<u8>,
    count: u8
}

impl Avg {
    pub fn new(data_type: DataType) -> Self {
        Avg {
            data_type,
            sum: vec!(),
            count: 0
        }
    }
}

impl AggregationTrait for Avg {
    fn get_name(&self) -> &'static str {
        "AVG"
    }

    fn initialize(&mut self) -> () {
        self.sum = vec!();
        self.count = 0;
    }

    fn consider_value(&mut self, value: Vec<u8>) -> () {
        self.sum = self.data_type.sum(&self.sum, &value).0;
        self.count += 1;
    }

    // TODO: Not implemented (currently have no way to do division)
    fn output(&self) -> (Vec<u8>) {
        panic!("Average not supported due to lack of a floating-point type");
    }
}

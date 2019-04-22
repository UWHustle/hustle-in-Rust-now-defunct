use logical_entities::aggregations::AggregationTrait;
use type_system::integer::*;
use type_system::data_type::*;
use type_system::*;

#[derive(Clone, Debug)]
pub struct Avg {
    sum: Box<Numeric>,
    count: i32,
}

impl Avg {
    pub fn new(data_type: DataType) -> Self {
        Avg {
            sum: data_type.create_zero(),
            count: 0,
        }
    }
}

impl AggregationTrait for Avg {
    fn get_name(&self) -> &'static str {
        "AVG"
    }

    fn initialize(&mut self) {
        self.sum = self.sum.data_type().create_zero();
        self.count = 0;
    }

    fn consider_value(&mut self, value: &Value) {
        self.sum = self.sum.add(force_numeric(value));
        self.count += 1;
    }

    fn output(&self) -> Box<Value> {
        let denom = Int4::from(self.count);
        self.sum.divide(&denom).box_clone_value()
    }

    fn output_type(&self) -> DataType {
        let denom = Int4::from(1);
        self.sum.divide(&denom).data_type()
    }
}

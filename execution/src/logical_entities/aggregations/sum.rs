use logical_entities::aggregations::AggregationTrait;
use hustle_types::data_type::*;
use hustle_types::force_numeric;
use hustle_types::Numeric;
use hustle_types::*;

#[derive(Clone, Debug)]
pub struct Sum {
    running_total: Box<Numeric>,
}

impl Sum {
    pub fn new(data_type: DataType) -> Self {
        Sum {
            running_total: data_type.create_zero(),
        }
    }
}

impl AggregationTrait for Sum {
    fn get_name(&self) -> &'static str {
        "SUM"
    }

    fn initialize(&mut self) {
        self.running_total = self.output_type().create_zero();
    }

    fn consider_value(&mut self, value: &Value) {
        self.running_total = self.running_total.add(force_numeric(value));
    }

    fn output(&self) -> Box<Value> {
        self.running_total.box_clone_value()
    }

    fn output_type(&self) -> DataType {
        self.running_total.data_type()
    }
}

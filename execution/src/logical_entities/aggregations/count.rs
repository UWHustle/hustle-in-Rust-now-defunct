use logical_entities::aggregations::AggregationTrait;
use type_system::data_type::*;
use type_system::integer::*;
use type_system::*;

#[derive(Clone, Debug)]
pub struct Count {
    count: Box<Numeric>,
}

impl Count {
    pub fn new(data_type: DataType) -> Self {
        Count {
            count: data_type.create_zero(),
        }
    }
}

impl AggregationTrait for Count {
    fn get_name(&self) -> &'static str {
        "COUNT"
    }

    fn initialize(&mut self) {
        self.count = self.count.data_type().create_zero();
    }

    #[allow(unused_variables)]
    fn consider_value(&mut self, value: &Value) {
        self.count = self.count.add(&Int2::from(1));
    }

    fn output(&self) -> Box<Value> {
        self.count.box_clone_value()
    }

    fn output_type(&self) -> DataType {
        self.count.data_type()
    }
}

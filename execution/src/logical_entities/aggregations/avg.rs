use logical_entities::aggregations::AggregationTrait;
use logical_entities::types::ValueType;
use logical_entities::types::Numeric;
use logical_entities::types::TypeID;
use logical_entities::types::float::*;

#[derive(Clone, Debug, PartialEq)]
pub struct Avg {
    sum: Box<Numeric>,
    count: u32,
}

impl Avg {
    pub fn new(data_type: TypeID) -> Self {
        Avg {
            sum: Numeric::zero(data_type),
            count: 0,
        }
    }
}

impl AggregationTrait for Avg {
    fn get_name(&self) -> &'static str {
        "AVG"
    }

    fn initialize(&mut self) -> () {
            self.sum = Numeric::zero(self.sum.type_id());
            self.count = 0;
        }

    fn consider_value(&mut self, value: &ValueType) -> () {
        self.sum = self.sum.add(value);
        self.count += 1;
    }

    // TODO: Not implemented (currently have no way to do division)
    fn output(&self) -> Box<ValueType> {
        self.sum.divide(self.count)
    }

    fn output_type(&self) -> TypeID {
        self.sum.divide(self.count).type_id()
    }
}
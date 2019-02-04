use logical_entities::aggregations::AggregationTrait;
use logical_entities::types::ValueType;

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

    fn consider_value(&mut self, value: ValueType) -> () {
        self.sum = self.sum.add(value);
        self.count += 1;
    }

    // TODO: Not implemented (currently have no way to do division)
    fn output(&self) -> (Box<Float>) {
        sum.divide(count)
    }

    fn output_type(&self) -> TypeID {
        sum.divide(count).type_id()
    }
}

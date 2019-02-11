use logical_entities::aggregations::AggregationTrait;
use logical_entities::types::ValueType;
use logical_entities::types::Numeric;
use logical_entities::types::TypeID;
use logical_entities::types::integer::*;
use logical_entities::types::force_numeric;

pub struct Avg {
    sum: Box<Numeric>,
    count: u32,
}

impl Avg {
    pub fn new(data_type: TypeID) -> Self {
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

    fn initialize(&mut self) -> () {
        self.sum = self.sum.type_id().create_zero();
        self.count = 0;
    }

    fn consider_value(&mut self, value: &ValueType) -> () {
        // TODO: Need a way to convert this to numeric type
        self.sum = self.sum.add(force_numeric(value));
        self.count += 1;
    }

    // TODO: Not implemented (currently have no way to do division)
    fn output(&self) -> &ValueType {
        let denom = Int4::new(self.count as i32);
        self.sum.divide(&denom).as_value_type()
    }

    fn output_type(&self) -> TypeID {
        self.output().type_id()
    }
}

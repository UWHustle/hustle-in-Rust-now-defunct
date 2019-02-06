use logical_entities::aggregations::AggregationTrait;
use logical_entities::types::TypeID;
use logical_entities::types::Numeric;
use logical_entities::types::ValueType;
use logical_entities::types::integer::*;
use logical_entities::types::float::*;

#[derive(Clone, Debug, PartialEq)]
pub struct Sum {
    running_total: Box<Numeric>,
}

impl Sum {
    pub fn new(type_id: TypeID) -> Self {
        Sum { running_total: Numeric::zero(type_id) }
    }
}

impl AggregationTrait for Sum {
    fn get_name(&self) -> &'static str {
        "SUM"
    }

    fn initialize(&mut self) -> () {
        self.running_total = match self.data_type {
            TypeID::Int2() => {
                Int8::zero()
            }
            TypeID::Int4() => {
                Int8::zero()
            }
            TypeID::Int8() => {
                Int8::zero()
            }
            TypeID::Float4() => {
                Float8::zero()
            }
            TypeID::Float8() => {
                Float8::zero()
            }
        }
    }

    fn consider_value(&mut self, value: &ValueType) -> () {
        self.running_total = self.running_total.add(&value);
    }

    fn output(&self) -> Box<ValueType> {
        self.running_total.clone()
    }

    fn output_type(&self) -> TypeID {
        self.data_type.clone()
    }
}

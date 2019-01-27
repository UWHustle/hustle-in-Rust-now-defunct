
pub mod integer;
pub mod ip_address;

use logical_entities::value::Value;

#[derive(Clone, Debug, PartialEq, Hash, Eq)]
pub enum DataType {
    Integer,
    IpAddress,
}

impl DataType {
    pub fn parse_to_value(&self, input: String) -> Value {
        Value::new(self.clone(),self.parse_and_marshall(input).0)
    }

    pub fn get_next_length(&self, payload: &[u8]) -> usize {
        match self {
            DataType::Integer => integer::IntegerType::get_next_length(payload),
            DataType::IpAddress => ip_address::IpAddressType::get_next_length(payload),
        }
    }

    pub fn parse_and_marshall(&self, input: String) -> (Vec<u8>,usize) {
        match self {
            DataType::Integer => integer::IntegerType::parse_and_marshall(input),
            DataType::IpAddress => ip_address::IpAddressType::parse_and_marshall(input),
        }
    }

    pub fn sum(&self, left:&Vec<u8>, right:&Vec<u8>) -> (Vec<u8>,usize) {
        match self {
            DataType::Integer => integer::IntegerType::sum(left, right),
            DataType::IpAddress => ip_address::IpAddressType::sum(left, right),
        }
    }

    pub fn compare(&self, left:&Vec<u8>, right:&Vec<u8>) -> i8 {
        match self {
            DataType::Integer => integer::IntegerType::compare(left, right),
            DataType::IpAddress => ip_address::IpAddressType::compare(left, right),
        }
    }


    pub fn to_string(&self, payload: &Vec<u8>) -> String {
        match self {
            DataType::Integer => integer::IntegerType::to_string(payload),
            DataType::IpAddress => ip_address::IpAddressType::to_string(payload),
        }
    }

    pub fn type_string(&self) -> String{
        match self {
            DataType::Integer => String::from("Int"),
            DataType::IpAddress => String::from("IP Address"),
        }
    }

}


pub trait DataTypeTrait {
    fn get_next_length(payload: &[u8]) -> usize;

    fn parse_and_marshall(input: String) -> (Vec<u8>,usize);

    fn sum(left:&Vec<u8>, right:&Vec<u8>) -> (Vec<u8>,usize);

    fn compare(left:&Vec<u8>, right:&Vec<u8>) -> i8;

    fn to_string(payload: &Vec<u8>) -> String;
}
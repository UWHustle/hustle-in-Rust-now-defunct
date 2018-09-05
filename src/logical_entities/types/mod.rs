
pub mod integer;
pub mod ip_address;

#[derive(Clone, Debug, PartialEq)]
pub enum DataType {
    Integer,
    IpAddress,
}

impl DataType {
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

    pub fn to_string(&self, payload: &Vec<u8>) -> String {
        match self {
            DataType::Integer => integer::IntegerType::to_string(payload),
            DataType::IpAddress => ip_address::IpAddressType::to_string(payload),
        }
    }

}


pub trait DataTypeTrait {
    fn get_next_length(payload: &[u8]) -> usize;

    fn parse_and_marshall(input: String) -> (Vec<u8>,usize);

    fn sum(left:&Vec<u8>, right:&Vec<u8>) -> (Vec<u8>,usize);

    fn to_string(payload: &Vec<u8>) -> String;
}
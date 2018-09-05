use std::mem;
use std::ptr;
use logical_entities::types::DataTypeTrait;

#[derive(Clone, Debug, PartialEq)]
pub struct IntegerType;

impl IntegerType {
    fn marshall<'a>(payload: &'a u64) -> (Vec<u8>,usize) {
        let array_representation;
        unsafe {
            array_representation = mem::transmute::<u64, [u8; 8]>(*payload);
        }
        (array_representation.to_vec(), 8)
    }

    fn unmarshall(payload: &Vec<u8>) -> u64 {
        if payload.len() == 0 {
            return 0;
        }

        let buffer = payload.as_slice();
        let c_buf = buffer.as_ptr();
        let s = c_buf as *mut u64;

        let s_safe = if mem::size_of::<u64>() <= mem::size_of_val(buffer) {
            unsafe {
                let ref s2 = *s;
                Some(s2)
            }
        } else {
            None
        };
        let value = *s_safe.unwrap();
        value
    }
}

impl DataTypeTrait for IntegerType {
    fn get_next_length(payload: &[u8]) -> usize {
        return 8;
    }

    fn parse_and_marshall(input: String) -> (Vec<u8>,usize) {
        let a = input.parse::<u64>().unwrap();
        IntegerType::marshall(&a)
    }

    fn sum(left:&Vec<u8>, right:&Vec<u8>) -> (Vec<u8>,usize) {
        let left_u64 = IntegerType::unmarshall(left);
        let right_u64 = IntegerType::unmarshall(right);
        IntegerType::marshall(&(left_u64+right_u64))
    }

    fn to_string(payload: &Vec<u8>) -> String {
        format!("{}", IntegerType::unmarshall(payload))
    }
}
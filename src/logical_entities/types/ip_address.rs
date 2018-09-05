use std::mem;
use logical_entities::types::DataTypeTrait;

#[derive(Clone, Debug, PartialEq)]
pub struct IpAddressType;

impl IpAddressType {
    fn marshall<'a>(payload: &'a u32) -> (Vec<u8>, usize){
        let array_representation;
        unsafe {
            array_representation = mem::transmute::<u32, [u8; 4]>(*payload);
        }
        (array_representation.to_vec(),4)
    }

    fn unmarshall(payload: &Vec<u8>) -> u32 {
        if payload.len() == 0 {
            return 0;
        }

        let buffer = payload.as_slice();
        let c_buf = buffer.as_ptr();
        let s = c_buf as *mut u32;

        let s_safe = if mem::size_of::<u32>() <= mem::size_of_val(buffer) {
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

impl DataTypeTrait for IpAddressType {
    fn parse_and_marshall(input: String) -> (Vec<u8>, usize) {
        let a = input.parse::<u32>().unwrap();
        IpAddressType::marshall(&a)
    }

    #[allow(unused_variables)]
    fn get_next_length(payload: &[u8]) -> usize {
        4
    }


    fn sum(left:&Vec<u8>, right:&Vec<u8>) -> (Vec<u8>,usize) {
        let left_u64 = IpAddressType::unmarshall(left);
        let right_u64 = IpAddressType::unmarshall(right);
        IpAddressType::marshall(&(left_u64+right_u64))
    }

    fn to_string(payload: &Vec<u8>) -> String {
        format!("{}", IpAddressType::unmarshall(payload))
    }

}
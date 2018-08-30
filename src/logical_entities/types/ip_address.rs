use std::mem;


pub struct IpAddressType;

impl IpAddressType {
    pub fn marshall<'a>(payload: &'a u32) -> (Vec<u8>){
        let array_representation;
        unsafe {
            array_representation = mem::transmute::<u32, [u8; 4]>(*payload);
        }
        array_representation.to_vec()
    }

    pub fn parse_and_marshall(input: String) -> (Vec<u8>) {
        let a = input.parse::<u32>().unwrap();
        IpAddressType::marshall(&a)
    }
}
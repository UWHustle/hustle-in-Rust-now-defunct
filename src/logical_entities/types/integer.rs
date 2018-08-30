use std::mem;


pub struct IntegerType;

impl IntegerType {
    pub fn get_size() -> usize {
        8
    }

    pub fn marshall<'a>(payload: &'a u64) -> (Vec<u8>){
        let array_representation;
        unsafe {
            array_representation = mem::transmute::<u64, [u8; 8]>(*payload);
        }
        array_representation.to_vec()
    }

    pub fn parse_and_write(input: String) -> (Vec<u8>) {
        let a = input.parse::<u64>().unwrap();
        IntegerType::marshall(&a)
    }
}
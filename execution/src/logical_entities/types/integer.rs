use std::mem;
use logical_entities::types::DataTypeTrait;

#[derive(Clone, Debug, PartialEq)]
pub struct IntegerType;

impl IntegerType {
    pub fn marshall<'a>(payload: &'a u64) -> (Vec<u8>,usize) {
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
    #[allow(unused_variables)]
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

    fn compare(left:&Vec<u8>, right:&Vec<u8>) -> i8 {
        let left_u64 = IntegerType::unmarshall(left) as i64;
        let right_u64 = IntegerType::unmarshall(right) as i64;
        println!("{}", left_u64);
        println!("{}", right_u64);
        let diff:i64 = left_u64 - right_u64 ;
        if diff<0{
            return -1;}
        else if diff==0{
            return 0;}
        else {return 1};
    }

    fn to_string(payload: &Vec<u8>) -> String {
        format!("{}", IntegerType::unmarshall(payload))
    }
}


#[cfg(test)]
mod tests {
    use logical_entities::types::integer::IntegerType;
    use logical_entities::types::DataTypeTrait;

    #[test]
    fn parse_and_marshall() {
        let (marshalled, _size) = IntegerType::parse_and_marshall("1".to_string());

        let unmarshalled = IntegerType::to_string(&marshalled);
        assert_eq!(unmarshalled,"1");
    }

    #[test]
    fn get_length() {
        let (marshalled, size) = IntegerType::parse_and_marshall("2".to_string());
        assert_eq!(marshalled.len(), size);
        assert_eq!(marshalled.len(), 8);
    }

    #[test]
    fn sum() {
        let (marshalled_a, _size) = IntegerType::parse_and_marshall("3".to_string());
        let (marshalled_b, _size) = IntegerType::parse_and_marshall("5".to_string());

        let (marshalled_sum, _size) = IntegerType::sum(&marshalled_a, &marshalled_b);

        let unmarshalled_sum = IntegerType::to_string(&marshalled_sum);

        assert_eq!(unmarshalled_sum, "8");
    }

}
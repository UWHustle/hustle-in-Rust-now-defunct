use std::mem;
use logical_entities::types::DataTypeTrait;

#[derive(Clone, Debug, PartialEq)]
pub struct IpAddressType;

impl IpAddressType {
    fn marshall(payload: &u32) -> (Vec<u8>, usize){
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
        let split = input.split(".");
        let mut total = 0;
        for s in split {
            total = total << 8;
            total += (s.parse::<u8>().unwrap() & 0xFF) as u32;
        }
        IpAddressType::marshall(&total)
    }

    #[allow(unused_variables)]
    fn get_next_length(payload: &[u8]) -> usize {
        4
    }


    fn sum(left:&Vec<u8>, right:&Vec<u8>) -> (Vec<u8>,usize) {
        let left_u32 = IpAddressType::unmarshall(left);
        let right_u32 = IpAddressType::unmarshall(right);
        IpAddressType::marshall(&(left_u32+right_u32))
    }

    fn compare(left:&Vec<u8>, right:&Vec<u8>) -> i8 {
        //last four implementation
        if left[0]&15 == right[0]&15{
            return  1;}
        else {return 0;}
    }

    fn to_string(payload: &Vec<u8>) -> String {
        let ip = IpAddressType::unmarshall(payload);
        let bytes = [ip & 0xFF,
            (ip >> 8) & 0xFF,
            (ip >> 16) & 0xFF,
            (ip >> 24) & 0xFF];
        format!("{}.{}.{}.{}", bytes[3], bytes[2], bytes[1], bytes[0])
    }

}


#[cfg(test)]
mod tests {
    use logical_entities::types::ip_address::IpAddressType;
    use logical_entities::types::DataTypeTrait;
    use std::mem;

    #[test]
    fn parse_and_marshall() {
        let (marshalled, _size) = IpAddressType::parse_and_marshall("1.2.3.4".to_string());

        let unmarshalled = IpAddressType::to_string(&marshalled);
        assert_eq!(unmarshalled,"1.2.3.4");
    }

    #[test]
    fn get_length() {
        let (marshalled, size) = IpAddressType::parse_and_marshall("1.2.3.4".to_string());
        assert_eq!(marshalled.len(), size);
        assert_eq!(marshalled.len(), 4);
    }

    #[test]
    fn sum() {
        let (marshalled_a, _size) = IpAddressType::parse_and_marshall("1.2.3.4".to_string());
        let (marshalled_b, _size) = IpAddressType::parse_and_marshall("5.6.7.8".to_string());

        let (marshalled_sum, _size) = IpAddressType::sum(&marshalled_a, &marshalled_b);

        let unmarshalled_sum = IpAddressType::to_string(&marshalled_sum);

        assert_eq!(unmarshalled_sum, "6.8.10.12");
    }

    #[test]
    fn compare() {
        let (marshalled_a, _size) = IpAddressType::parse_and_marshall("12.25.37.48".to_string());
        let payload : &u64 = &48;
        let array_representation;
        unsafe {
            array_representation = mem::transmute::<u64, [u8; 8]>(*payload);
        }
        let vector : Vec<u8> =  (array_representation.to_vec(), 8).0;
        let check = IpAddressType::compare(&marshalled_a, &vector);
        println!("{}", marshalled_a[0]);
        println!("{}", vector[0]);
        assert_eq!(check, 1);
    }


}
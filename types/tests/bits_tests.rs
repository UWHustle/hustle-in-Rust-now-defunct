#[cfg(test)]
mod bits_tests {
    use hustle_types::{Bits, HustleType};

    #[test]
    #[should_panic(expected = "Cannot create Bits type with zero length")]
    fn empty() {
        Bits::new(0);
    }

    #[test]
    #[should_panic(expected = "Bit 1 out of range for length 1")]
    fn out_of_range() {
        let bits_type = Bits::new(1);
        let buf = vec![0u8; bits_type.byte_len()];
        bits_type.get(1, &buf);
    }

    #[test]
    fn byte_len() {
        assert_eq!(Bits::new(1).byte_len(), 1);
        assert_eq!(Bits::new(8).byte_len(), 1);
        assert_eq!(Bits::new(9).byte_len(), 2);
    }

    #[test]
    fn set_and_get() {
        let bits_type = Bits::new(9);
        let mut buf = vec![0u8; bits_type.byte_len()];

        bits_type.set(0, false, &mut buf);
        bits_type.set(1, true, &mut buf);
        bits_type.set(8, true, &mut buf);

        assert!(!bits_type.get(0, &buf));
        assert!(bits_type.get(1, &buf));
        assert!(bits_type.get(8, &buf));
    }
}

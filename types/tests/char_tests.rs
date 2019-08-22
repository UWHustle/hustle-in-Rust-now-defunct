#[cfg(test)]
mod char_tests {
    use hustle_types::{
        Char,
        CompareEq,
        CompareOrd,
        HustleType,
    };

    #[test]
    #[should_panic(expected = "Cannot create Char type with zero length")]
    fn empty() {
        Char::new(0);
    }

    #[test]
    #[should_panic]
    fn non_utf8() {
        let char_type = Char::new(1);
        let mut buf = vec![0u8; char_type.byte_len()];

        char_type.set("💣", &mut buf);
    }

    #[test]
    fn set_and_get() {
        let char_type = Char::new(8);
        let mut buf = vec![0u8; char_type.byte_len()];

        char_type.set("aardvark", &mut buf);
        assert_eq!(char_type.get(&buf), "aardvark");

        char_type.set("short", &mut buf);
        assert_eq!(char_type.get(&buf), "short\0\0\0");

        char_type.set("long strings are truncated", &mut buf);
        assert_eq!(char_type.get(&buf), "long str");
    }

    #[test]
    fn compare() {
        let char_type = Char::new(8);
        let mut buf_a = vec![0u8; char_type.byte_len()];
        let mut buf_b = vec![0u8; char_type.byte_len()];

        char_type.set("aardvark", &mut buf_a);
        char_type.set("zyzzyvas", &mut buf_b);

        assert!(char_type.eq(&char_type, &buf_a, &buf_a));

        assert!(char_type.lt(&char_type, &buf_a, &buf_b));
        assert!(char_type.le(&char_type, &buf_a, &buf_b));
        assert!(char_type.gt(&char_type, &buf_b, &buf_a));
        assert!(char_type.ge(&char_type, &buf_b, &buf_a));
    }
}

#[cfg(test)]
mod primitive_tests {
    use hustle_types::{
        Bool,
        CompareEq,
        CompareOrd,
        HustleType,
        Int16,
        Int32,
        Int64,
        Int8,
    };

    macro_rules! init_buf {
        ($hustle_type:expr, $primitive:expr) => {{
            let byte_len = $hustle_type.byte_len();
            assert_eq!(byte_len, std::mem::size_of_val(&$primitive));
            let mut buf = vec![0u8; byte_len];
            $hustle_type.set($primitive, &mut buf);
            assert_eq!($hustle_type.get(&buf), $primitive);
            buf
        }};
    }

    #[test]
    fn bool() {
        let bool_type = Bool;

        let false_buf = init_buf!(bool_type, false);
        let true_buf = init_buf!(bool_type, true);

        assert!(bool_type.eq(&bool_type, &false_buf, &false_buf)); // false == false
        assert!(bool_type.eq(&bool_type, &true_buf, &true_buf)); // true == true
        assert!(!bool_type.eq(&bool_type, &false_buf, &true_buf)); // !(false == true)
        assert!(bool_type.lt(&bool_type, &false_buf, &true_buf)); // false < true
        assert!(bool_type.le(&bool_type, &false_buf, &true_buf)); // false <= true
        assert!(bool_type.le(&bool_type, &false_buf, &false_buf)); // false <= false
        assert!(bool_type.gt(&bool_type, &true_buf, &false_buf)); // true > false
        assert!(bool_type.ge(&bool_type, &true_buf, &false_buf)); // true >= false
        assert!(bool_type.ge(&bool_type, &true_buf, &true_buf)); // true >= true
    }

    #[test]
    fn int() {
        let int8_type = Int8;
        let int16_type = Int16;
        let int32_type = Int32;
        let int64_type = Int64;

        let int8_min_buf = init_buf!(int8_type, std::i8::MIN);
        let int8_zero_buf = init_buf!(int8_type, 0i8);
        let int8_max_buf = init_buf!(int8_type, std::i8::MAX);

        let int16_min_buf = init_buf!(int16_type, std::i16::MIN);
        let int16_zero_buf = init_buf!(int16_type, 0i16);
        let int16_max_buf = init_buf!(int16_type, std::i16::MAX);

        let int32_min_buf = init_buf!(int32_type, std::i32::MIN);
        let int32_zero_buf = init_buf!(int32_type, 0i32);
        let int32_max_buf = init_buf!(int32_type, std::i32::MAX);

        let int64_min_buf = init_buf!(int64_type, std::i64::MIN);
        let int64_zero_buf = init_buf!(int64_type, 0i64);
        let int64_max_buf = init_buf!(int64_type, std::i64::MAX);

        assert!(int8_type.lt(&int8_type, &int8_min_buf, &int8_zero_buf));
        assert!(int8_type.lt(&int8_type, &int8_zero_buf, &int8_max_buf));

        assert!(int16_type.lt(&int16_type, &int16_min_buf, &int16_zero_buf));
        assert!(int16_type.lt(&int16_type, &int16_zero_buf, &int16_max_buf));

        assert!(int32_type.lt(&int32_type, &int32_min_buf, &int32_zero_buf));
        assert!(int32_type.lt(&int32_type, &int32_zero_buf, &int32_max_buf));

        assert!(int64_type.lt(&int64_type, &int64_min_buf, &int64_zero_buf));
        assert!(int64_type.lt(&int64_type, &int64_zero_buf, &int64_max_buf));

        assert!(int8_type.gt(&int16_type, &int8_min_buf, &int16_min_buf));
        assert!(int16_type.gt(&int32_type, &int16_min_buf, &int32_min_buf));
        assert!(int32_type.gt(&int64_type, &int32_min_buf, &int64_min_buf));

        assert!(int8_type.lt(&int16_type, &int8_max_buf, &int16_max_buf));
        assert!(int16_type.lt(&int32_type, &int16_max_buf, &int32_max_buf));
        assert!(int32_type.lt(&int64_type, &int32_max_buf, &int64_max_buf));
    }
}

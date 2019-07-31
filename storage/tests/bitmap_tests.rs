extern crate hustle_storage;

#[cfg(test)]
mod bitmap_tests {
    use hustle_storage::block::BitMap;
    use std::convert::TryInto;

    #[test]
    fn get() {
        let bm = BitMap::new(vec![0]);
        assert_eq!(bm.get_unchecked(0), false);

        let t = &[0u8; 8];
        let (int_bytes, rest) =
        let val = u32::from_le_bytes(t_t);
    }

    #[test]
    fn set() {
        let mut bm = BitMap::new(vec![0]);

        bm.set_unchecked(0, true);
        assert_eq!(bm.get_unchecked(0), true);

        bm.set_unchecked(0, false);
        assert_eq!(bm.get_unchecked(0), false);
    }

    #[test]
    fn iter() {
        let mut bm = BitMap::new(vec![0]);

        bm.set_unchecked(7, true);

        let mut iter = bm.iter();
        for _ in 0..7 {
            assert_eq!(iter.next(), Some(false));
        }
        assert_eq!(iter.next(), Some(true));
        assert_eq!(iter.next(), None);
    }
}
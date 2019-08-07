extern crate hustle_storage;

#[cfg(test)]
mod bitmap_tests {
    use hustle_storage::block::BitMap;

    #[test]
    fn get() {
        let mut v = vec![0];
        let bm = BitMap::new(&mut v);
        assert_eq!(bm.get_unchecked(0), false);
    }

    #[test]
    fn set() {
        let mut v = vec![0];
        let bm = BitMap::new(&mut v);

        bm.set_unchecked(0, true);
        assert_eq!(bm.get_unchecked(0), true);

        bm.set_unchecked(0, false);
        assert_eq!(bm.get_unchecked(0), false);
    }

    #[test]
    fn iter() {
        let mut v = vec![0];
        let bm = BitMap::new(&mut v);

        bm.set_unchecked(7, true);

        let mut iter = bm.iter();
        for _ in 0..7 {
            assert_eq!(iter.next(), Some(false));
        }
        assert_eq!(iter.next(), Some(true));
        assert_eq!(iter.next(), None);
    }
}

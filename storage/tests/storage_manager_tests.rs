extern crate storage;
extern crate memmap;

#[cfg(test)]
#[allow(unused_must_use)]
mod storage_manager_tests {
    use storage::StorageManager;

    #[test]
    fn get() {
        let sm = StorageManager::new();
        sm.put("key_get", b"value");
        assert_eq!(&sm.get("key_get").unwrap().get_block(0).unwrap()[0..5], b"value");
        assert!(sm.get("nonexistent_key").is_none());
        sm.delete("key_get");
    }

    #[test]
    fn put() {
        let sm = StorageManager::new();
        sm.put("key_put", b"value");
        assert_eq!(&sm.get("key_put").unwrap().get_block(0).unwrap()[0..5], b"value");
        sm.put("key_put", b"new_value");
        assert_eq!(&sm.get("key_put").unwrap().get_block(0).unwrap()[0..9], b"new_value");
        sm.delete("key_put");
    }

    #[test]
    fn put_anon() {
        let sm = StorageManager::new();
        let key_0 = sm.put_anon(b"value");
        assert_eq!(&sm.get(&key_0).unwrap().get_block(0).unwrap()[0..5], b"value");
        sm.delete(&key_0);

        let key_1 = sm.put_anon(b"");
        assert_eq!(sm.get(&key_1).unwrap().get_block(0).unwrap().len(), 0);
        sm.delete(&key_1);
    }

    #[test]
    fn delete() {
        let sm = StorageManager::new();
        sm.put("key_delete", b"value");
        assert_eq!(&sm.get("key_delete").unwrap().get_block(0).unwrap()[0..5], b"value");
        sm.delete("key_delete");
        assert!(sm.get("key_delete").is_none());
    }

    #[test]
    fn col_iter() {
        let sm = StorageManager::new();
        sm.put("key_col_iter", b"abbcdd");

        {
            let record = sm.get_with_schema("key_col_iter", &[1, 2]).unwrap();
            let block = record.get_block(0).unwrap();

            let mut col_iter = block.get_col(0).unwrap();
            assert_eq!(&col_iter.next().unwrap(), &b"a");
            assert_eq!(&col_iter.next().unwrap(), &b"c");
            assert!(col_iter.next().is_none());
        }

        sm.delete("key_col_iter");
    }

    #[test]
    fn row_iter() {
        let sm = StorageManager::new();
        sm.put("key_row_iter", b"abbcdd");

        {
            let record = sm.get_with_schema("key_row_iter", &[1, 2]).unwrap();
            let block = record.get_block(0).unwrap();
            let mut row_iter = block.get_row(0).unwrap();
            assert_eq!(&row_iter.next().unwrap(), &b"a");
            assert_eq!(&row_iter.next().unwrap(), &b"bb");
            assert!(row_iter.next().is_none());
        }

        sm.delete("key_row_iter");
    }

    #[test]
    fn get_row_col() {
        let sm = StorageManager::new();
        sm.put("key_get_row_col", b"abbcdd");

        {
            let record = sm.get_with_schema("key_get_row_col", &[1, 2]).unwrap();
            let block = record.get_block(0).unwrap();
            assert_eq!(&block.get_row_col(0, 0).unwrap(), &b"a");
            assert_eq!(&block.get_row_col(0, 1).unwrap(), &b"bb");
            assert_eq!(&block.get_row_col(1, 0).unwrap(), &b"c");
            assert_eq!(&block.get_row_col(1, 1).unwrap(), &b"dd");
            assert!(block.get_row_col(0, 2).is_none());
            assert!(block.get_row_col(2, 0).is_none());
        }

        sm.delete("key_get_row_col");
    }

    #[test]
    fn set_row_col() {
        let sm = StorageManager::new();
        sm.put("key_set_row_col", b"abbcdd");

        {
            let record = sm.get_with_schema("key_set_row_col", &[1, 2]).unwrap();
            let block = record.get_block(0).unwrap();
            block.set_row_col(0, 0, b"e");
            block.set_row_col(1, 1, b"ff");
            assert_eq!(&block[0..6], b"ebbcff");
        }

        sm.delete("key_set_row_col");
    }

    #[test]
    fn append() {
        let sm = StorageManager::new();
        sm.delete("key_append");
        sm.append("key_append", b"a");
        assert_eq!(&sm.get("key_append").unwrap().get_block(0).unwrap()[0..1], b"a");
        sm.append("key_append", b"bb");
        assert_eq!(&sm.get("key_append").unwrap().get_block(0).unwrap()[0..3], b"abb");
        sm.delete("key_append");
    }
}

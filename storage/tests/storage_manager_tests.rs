extern crate storage;
extern crate memmap;
extern crate core;

#[cfg(test)]
#[allow(unused_must_use)]
mod storage_manager_tests {
    use storage::StorageManager;

    #[test]
    fn get() {
        let sm = StorageManager::new();
        let kv_engine = sm.key_value_engine();
        kv_engine.put("key_get", b"value");
        assert_eq!(&kv_engine.get("key_get").unwrap()[0..5], b"value");
        assert!(kv_engine.get("nonexistent_key").is_none());
        kv_engine.delete("key_get");
    }

    #[test]
    fn put() {
        let sm = StorageManager::new();
        let kv_engine = sm.key_value_engine();
        kv_engine.put("key_put", b"value");
        assert_eq!(&kv_engine.get("key_put").unwrap()[0..5], b"value");
        kv_engine.put("key_put", b"new_value");
        assert_eq!(&kv_engine.get("key_put").unwrap()[0..9], b"new_value");
        kv_engine.delete("key_put");
    }

    #[test]
    fn put_anon() {
        let sm = StorageManager::new();
        let kv_engine = sm.key_value_engine();
        let key = kv_engine.put_anon(b"value");
        assert_eq!(&kv_engine.get(&key).unwrap()[0..5], b"value");
        kv_engine.delete(&key);
    }

    #[test]
    fn delete() {
        let sm = StorageManager::new();
        let kv_engine = sm.key_value_engine();
        kv_engine.put("key_delete", b"value");
        assert_eq!(&kv_engine.get("key_delete").unwrap()[0..5], b"value");
        kv_engine.delete("key_delete");
        assert!(kv_engine.get("key_delete").is_none());
    }

    #[test]
    fn get_row_col() {
        let sm = StorageManager::new();
        let rl_engine = sm.relational_engine();
        let pr = rl_engine.create("key_get_row_col", vec![1, 2]);
        pr.bulk_write(b"abbcdd");

        {
            let block = pr.get_block(0).unwrap();
            assert_eq!(&block.get_row_col(0, 0).unwrap(), &b"a");
            assert_eq!(&block.get_row_col(0, 1).unwrap(), &b"bb");
            assert_eq!(&block.get_row_col(1, 0).unwrap(), &b"c");
            assert_eq!(&block.get_row_col(1, 1).unwrap(), &b"dd");
            assert!(block.get_row_col(0, 2).is_none());
            assert!(block.get_row_col(2, 0).is_none());
        }

        rl_engine.drop("key_get_row_col");
    }

    #[test]
    fn set_row_col() {
        let sm = StorageManager::new();
        let rl_engine = sm.relational_engine();
        let pr = rl_engine.create("key_set_row_col", vec![1, 2]);
        pr.bulk_write(b"abbcdd");

        {
            let block = pr.get_block(0).unwrap();
            block.set_row_col(0, 0, b"e");
            block.set_row_col(1, 1, b"ff");
            assert_eq!(&block.bulk_read()[0..6], b"ebbcff");
        }

        rl_engine.drop("key_set_row_col");
    }

    #[test]
    fn len() {
        let sm = StorageManager::new();
        let rl_engine = sm.relational_engine();
        let pr = rl_engine.create("key_len", vec![1]);

        assert_eq!(pr.get_block(0).unwrap().get_n_rows(), 0);

        let mut row_builder = pr.insert_row();
        row_builder.push(b"a");

        assert_eq!(pr.get_block(0).unwrap().get_n_rows(), 1);

        rl_engine.drop("key_len");
    }
}

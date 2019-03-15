extern crate storage;

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
        let key = sm.put_anon(b"value");
        assert_eq!(&sm.get(&key).unwrap().get_block(0).unwrap()[0..5], b"value");
        sm.delete(&key);
    }

    #[test]
    fn delete() {
        let sm = StorageManager::new();
        sm.put("key_delete", b"value");
        assert_eq!(&sm.get("key_delete").unwrap().get_block(0).unwrap()[0..5], b"value");
        sm.delete("key_delete");
        assert!(sm.get("key_delete").is_none());
    }
}

extern crate storage;

#[cfg(test)]
#[allow(unused_must_use)]
mod storage_manager_tests {
    use storage::StorageManager;
    use std::mem;

    #[test]
    fn get() {
        let sm = StorageManager::new();
        sm.put("key_get", b"value");
        assert_eq!(&sm.get("key_get").unwrap()[0..5], b"value");
        assert!(sm.get("nonexistent_key").is_none());
        sm.delete("key_get");
    }

    #[test]
    fn put() {
        let sm = StorageManager::new();
        sm.put("key_put", b"value");
        assert_eq!(&sm.get("key_put").unwrap()[0..5], b"value");
        sm.put("key_put", b"new_value");
        assert_eq!(&sm.get("key_put").unwrap()[0..9], b"new_value");
        sm.delete("key_put");
    }

    #[test]
    fn put_anon() {
        let sm = StorageManager::new();
        let key = sm.put_anon(b"value");
        assert_eq!(&sm.get(key.as_str()).unwrap()[0..5], b"value");
        sm.delete(key.as_str());
    }

    #[test]
    fn delete() {
        let sm = StorageManager::new();
        sm.put("key_delete", b"value");
        assert_eq!(&sm.get("key_delete").unwrap()[0..5], b"value");
        sm.delete("key_delete");
        assert!(sm.get("key_delete").is_none());
    }

    #[test]
    fn replacement() {
        let sm = StorageManager::with_capacity(3);
        sm.put("key_replacement_1", b"value");
        sm.put("key_replacement_2", b"value");
        sm.put("key_replacement_3", b"value");
        sm.put("key_replacement_4", b"value");

        // Record 4 is most recently used so it should be in the sm.
        // Record 1 is least recently used so it should have been replaced.
        assert!(sm.is_cached("key_replacement_4"));
        assert!(!sm.is_cached("key_replacement_1"));
        sm.delete("key_replacement_1");
        sm.delete("key_replacement_2");
        sm.delete("key_replacement_3");
        sm.delete("key_replacement_4");
    }

    #[test]
    fn reference() {
        let sm = StorageManager::with_capacity(3);

        sm.put("key_reference_1", b"value");
        let value = sm.get("key_reference_1");

        sm.put("key_reference_2", b"value");
        sm.put("key_reference_3", b"value");
        sm.put("key_reference_4", b"value");

        // We have a reference to the record 1 so it should not have been replaced.
        assert!(sm.is_cached("key_reference_4"));
        assert!(sm.is_cached("key_reference_1"));

        // Instead record 2 is is least recently used without a reference.
        assert!(sm.is_cached("key_reference_2"));

        mem::drop(value);
        sm.delete("key_reference_1");
        sm.delete("key_reference_2");
        sm.delete("key_reference_3");
        sm.delete("key_reference_4");
    }
}

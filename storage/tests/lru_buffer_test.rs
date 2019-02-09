extern crate storage;

#[cfg(test)]
#[allow(unused_must_use)]
mod tests {
    use storage::lru_buffer::LruBuffer;
    use std::slice;

    #[test]
    fn put_get() {
        let mut buffer = LruBuffer::new(1, 1000);
        let key = "k_put_get";
        let expected_value = b"value";
        buffer.put(key, expected_value);
        get_and_assert_eq(&mut buffer, key, expected_value);
        buffer.delete(key);
    }

    #[test]
    fn put_update_get() {
        let mut buffer = LruBuffer::new(1, 1000);
        let key = "k_put_update_get";
        let expected_value = b"value";
        buffer.put(key, b"old_value");
        buffer.put(key, expected_value);
        get_and_assert_eq(&mut buffer, key, expected_value);
        buffer.delete(key);
    }

    #[test]
    #[should_panic]
    fn get_nonexistent_key() {
        LruBuffer::new(1, 1000).get("nonexistent_key").unwrap();
    }

    #[test]
    #[should_panic]
    fn delete_get() {
        let mut buffer = LruBuffer::new(1, 1000);
        let key = "k_delete";
        let value = b"value";
        buffer.put(key, value);
        buffer.delete(key);
        buffer.get(key).unwrap();
    }

    #[test]
    #[should_panic]
    fn pin_nonexistent_key() {
        LruBuffer::new(1, 1000).pin("nonexistent_key");
    }

    #[test]
    fn pin_release() {
        let mut buffer = LruBuffer::new(1, 1000);
        let key1 = "k_pin_release_1";
        let key2 = "k_pin_release_2";
        let key3 = "k_pin_release_3";
        let expected_value: [u8; 500] = [0; 500];

        // Put all three records in the storage manager
        buffer.put(key1, &expected_value);
        buffer.put(key2, &expected_value);
        buffer.put(key3, &expected_value);

        // Get and pin a pointer to one record
        let (len, ptr) = buffer.get(key1).unwrap();
        buffer.pin(key1);

        // Load the other two records into the buffer
        buffer.get(key2);
        buffer.get(key3);

        // Assert that the pointer is still valid and points to the correct data
        assert!(!ptr.is_null());
        let actual_value = unsafe { slice::from_raw_parts(ptr, len) };
        assert_eq!(&expected_value[..], actual_value);

        buffer.delete(key1);
        buffer.delete(key2);
        buffer.delete(key3);
    }

    fn get_and_assert_eq(buffer: &mut LruBuffer, key: &str, expected_value: &[u8]) {
        let (len, ptr) = buffer.get(key).unwrap();
        let actual_value = unsafe { slice::from_raw_parts(ptr, len) };
        assert_eq!(expected_value, actual_value);
    }
}

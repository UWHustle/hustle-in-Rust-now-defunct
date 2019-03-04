extern crate storage;

#[cfg(test)]
#[allow(unused_must_use)]
mod buffer_tests {
    use storage::buffer::Buffer;
    use std::mem;

    #[test]
    fn get() {
        let buffer = Buffer::new();
        buffer.put("key_get", b"value");
        assert_eq!(&buffer.get("key_get").unwrap()[0..5], b"value");
        assert!(buffer.get("nonexistent_key").is_none());
        buffer.delete("key_get");
    }

    #[test]
    fn put() {
        let buffer = Buffer::new();
        buffer.put("key_put", b"value");
        assert_eq!(&buffer.get("key_put").unwrap()[0..5], b"value");
        buffer.put("key_put", b"new_value");
        assert_eq!(&buffer.get("key_put").unwrap()[0..9], b"new_value");
        buffer.delete("key_put");
    }

    #[test]
    fn put_anon() {
        let buffer = Buffer::new();
        let key = buffer.put_anon(b"value");
        assert_eq!(&buffer.get(key.as_str()).unwrap()[0..5], b"value");
        buffer.delete(key.as_str());
    }

    #[test]
    fn delete() {
        let buffer = Buffer::new();
        buffer.put("key_delete", b"value");
        assert_eq!(&buffer.get("key_delete").unwrap()[0..5], b"value");
        buffer.delete("key_delete");
        assert!(buffer.get("key_delete").is_none());
    }

    #[test]
    fn replacement() {
        let buffer = Buffer::with_capacity(3);
        buffer.put("key_replacement_1", b"value");
        buffer.put("key_replacement_2", b"value");
        buffer.put("key_replacement_3", b"value");
        buffer.put("key_replacement_4", b"value");

        // Record 4 is most recently used so it should be in the buffer.
        // Record 1 is least recently used so it should have been replaced.
        assert!(buffer.is_cached("key_replacement_4"));
        assert!(!buffer.is_cached("key_replacement_1"));
        buffer.delete("key_replacement_1");
        buffer.delete("key_replacement_2");
        buffer.delete("key_replacement_3");
        buffer.delete("key_replacement_4");
    }

    #[test]
    fn reference() {
        let buffer = Buffer::with_capacity(3);

        buffer.put("key_reference_1", b"value");
        let value = buffer.get("key_reference_1");

        buffer.put("key_reference_2", b"value");
        buffer.put("key_reference_3", b"value");
        buffer.put("key_reference_4", b"value");

        // We have a reference to the record 1 so it should not have been replaced.
        assert!(buffer.is_cached("key_reference_4"));
        assert!(buffer.is_cached("key_reference_1"));

        // Instead record 2 is is least recently used without a reference.
        assert!(buffer.is_cached("key_reference_2"));

        mem::drop(value);
        buffer.delete("key_reference_1");
        buffer.delete("key_reference_2");
        buffer.delete("key_reference_3");
        buffer.delete("key_reference_4");
    }
}

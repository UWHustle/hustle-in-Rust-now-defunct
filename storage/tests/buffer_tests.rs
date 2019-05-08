extern crate storage;

#[cfg(test)]
#[allow(unused_must_use)]
mod buffer_tests {
    use storage::Buffer;
    use std::mem;

    #[test]
    fn get() {
        let buffer = Buffer::with_capacity(10);
        buffer.write("key_get", b"value");
        assert_eq!(&buffer.get("key_get").unwrap()[0..5], b"value");
        assert!(buffer.get("nonexistent_key").is_none());
        buffer.erase("key_get");
    }

    #[test]
    fn write() {
        let buffer = Buffer::with_capacity(10);
        buffer.write("key_write", b"value");
        assert_eq!(&buffer.get("key_write").unwrap()[0..5], b"value");
        buffer.write("key_write", b"new_value");
        assert_eq!(&buffer.get("key_write").unwrap()[0..9], b"new_value");
        buffer.erase("key_write");
    }

    #[test]
    fn erase() {
        let buffer = Buffer::with_capacity(10);
        buffer.write("key_erase", b"value");
        assert_eq!(&buffer.get("key_erase").unwrap()[0..5], b"value");
        buffer.erase("key_erase");
        assert!(buffer.get("key_erase").is_none());
    }

    #[test]
    fn exists() {
        let buffer = Buffer::with_capacity(10);
        buffer.write("key_exists", b"value");
        assert!(buffer.exists("key_exists"));
        buffer.erase("key_exists");
        assert!(!buffer.exists("key_exists"));
    }

    #[test]
    fn replacement() {
        let buffer = Buffer::with_capacity(3);
        buffer.write("key_replacement_1", b"value");
        buffer.write("key_replacement_2", b"value");
        buffer.write("key_replacement_3", b"value");
        buffer.write("key_replacement_4", b"value");

        // Record 4 is most recently used so it should be in the cache.
        // Record 1 is least recently used so it should have been replaced.
        assert!(buffer.is_cached("key_replacement_4"));
        assert!(!buffer.is_cached("key_replacement_1"));
        buffer.erase("key_replacement_1");
        buffer.erase("key_replacement_2");
        buffer.erase("key_replacement_3");
        buffer.erase("key_replacement_4");
    }

    #[test]
    fn reference() {
        let buffer = Buffer::with_capacity(3);

        buffer.write("key_reference_1", b"value");
        let value = buffer.get("key_reference_1");

        buffer.write("key_reference_2", b"value");
        buffer.write("key_reference_3", b"value");
        buffer.write("key_reference_4", b"value");

        // We have a reference to the record 1 so it should not have been replaced.
        assert!(buffer.is_cached("key_reference_4"));
        assert!(buffer.is_cached("key_reference_1"));

        // Instead record 2 is is least recently used without a reference.
        assert!(buffer.is_cached("key_reference_2"));

        mem::drop(value);
        buffer.erase("key_reference_1");
        buffer.erase("key_reference_2");
        buffer.erase("key_reference_3");
        buffer.erase("key_reference_4");
    }
}

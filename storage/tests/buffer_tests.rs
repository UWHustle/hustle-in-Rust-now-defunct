extern crate storage;

#[cfg(test)]
#[allow(unused_must_use)]
mod buffer_tests {
    use storage::buffer::Buffer;

    #[test]
    fn get() {
        let buffer = Buffer::new();
        buffer.put("key_get", b"value");
        assert_eq!(&buffer.get("key_get").unwrap()[0..5], b"value");
        assert!(buffer.get("nonexistent_key").is_none());
        buffer.delete("key_get");
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
    }
}

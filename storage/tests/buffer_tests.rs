extern crate storage;

#[cfg(test)]
#[allow(unused_must_use)]
mod buffer_tests {
    use storage::buffer::Buffer;

    #[test]
    fn get() {
        let buffer = Buffer::new();
        buffer.put("key_get", b"value");
        assert_eq!(&buffer.get("key_get").unwrap().upgrade().unwrap()[0..5], b"value");
        assert!(buffer.get("nonexistent_key").is_none());
        buffer.delete("key_get");
    }

    #[test]
    fn delete() {
        let buffer = Buffer::new();
        buffer.put("key_delete", b"value");
        let value = buffer.get("key_delete").unwrap();
        assert_eq!(&value.upgrade().unwrap()[0..5], b"value");
        buffer.delete("key_delete");
        assert!(value.upgrade().is_none());
        assert!(buffer.get("key_delete").is_none());
    }
}

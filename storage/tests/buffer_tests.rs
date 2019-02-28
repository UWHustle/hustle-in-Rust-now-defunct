extern crate storage;

#[cfg(test)]
#[allow(unused_must_use)]
mod buffer_tests {
    use storage::buffer::Buffer;

    #[test]
    fn get() {
        let buffer = Buffer::new();
        buffer.put("key", b"value");
        assert_eq!(&buffer.get("key").unwrap()[0..5], b"value");
        assert!(buffer.get("nonexistent_key").is_none());
        buffer.delete("key");
    }
}

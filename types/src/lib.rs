#[macro_use]
extern crate serde;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Type {
    variant: TypeVariant,
    size: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TypeVariant {
    Int1,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    ByteString(usize, bool), // Represent (max_size, varchar)
    IPv4,
}

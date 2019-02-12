use super::*;

// All possible concrete types
// The boolean indicates whether the type is nullable (true if it is; false if it isn't)
#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub enum TypeID {
    Int2(bool),
    Int4(bool),
    Int8(bool),
    Float4(bool),
    Float8(bool),
    UTF8String(bool),
    IPv4(bool),
}

impl TypeID {
    // Returns the size of an object of this type; -1 if the size is variable
    pub fn size(&self) -> usize {
        match self {
            TypeID::Int2(nullable) => 2,
            TypeID::Int4(nullable) => 4,
            TypeID::Int8(nullable) => 8,
            TypeID::Float4(nullable) => 4,
            TypeID::Float8(nullable) => 8,
            TypeID::UTF8String(nullable) => 0,
            TypeID::IPv4(nullable) => 4,
        }
    }

    pub fn nullable(&self) -> &bool {
        match self {
            TypeID::Int2(nullable) => nullable,
            TypeID::Int4(nullable) => nullable,
            TypeID::Int8(nullable) => nullable,
            TypeID::Float4(nullable) => nullable,
            TypeID::Float8(nullable) => nullable,
            TypeID::UTF8String(nullable) => nullable,
            TypeID::IPv4(nullable) => nullable,
        }
    }

    pub fn create_null(&self) -> Box<ValueType> {
        match self {
            TypeID::Int2(nullable) => {
                Box::new(Int2::create_null())
            }
            TypeID::Int4(nullable) => {
                Box::new(Int4::create_null())
            }
            TypeID::Int8(nullable) => {
                Box::new(Int8::create_null())
            }
            TypeID::Float4(nullable) => {
                Box::new(Float4::create_null())
            }
            TypeID::Float8(nullable) => {
                Box::new(Float8::create_null())
            }
            TypeID::UTF8String(nullable) => {
                Box::new(UTF8String::create_null())
            }
            _ => panic!("Type {:?} cannot be NULL", self)
        }
    }

    pub fn parse(&self, string: &str) -> Box<ValueType> {
        match self {
            TypeID::Int2(nullable) => {
                Box::new(Int2::parse(string))
            }
            TypeID::Int4(nullable) => {
                Box::new(Int4::parse(string))
            }
            TypeID::Int8(nullable) => {
                Box::new(Int8::parse(string))
            }
            TypeID::Float4(nullable) => {
                Box::new(Float4::parse(string))
            }
            TypeID::Float8(nullable) => {
                Box::new(Float8::parse(string))
            }
            TypeID::IPv4(nullable) => {
                Box::new(IPv4::parse(string))
            }
            TypeID::UTF8String(nullable) => {
                Box::new(UTF8String::new(string))
            }
        }
    }

    pub fn create_zero(&self) -> Box<Numeric> {
        match self {
            TypeID::Int2(nullable) => {
                Box::new(Int2::new(0))
            }
            TypeID::Int4(nullable) => {
                Box::new(Int4::new(0))
            }
            TypeID::Int8(nullable) => {
                Box::new(Int8::new(0))
            }
            TypeID::Float4(nullable) => {
                Box::new(Float4::new(0.0))
            }
            TypeID::Float8(nullable) => {
                Box::new(Float8::new(0.0))
            }
            _ => panic!("Type {:?} is not numeric", self)
        }
    }

    pub fn from_string(string: String) -> TypeID {
        let string = string.to_lowercase();
        let id = match string.as_str() {
            "smallint" => TypeID::Int2(false),
            "smallint null" => TypeID::Int2(true),
            "int" => TypeID::Int4(false),
            "int null" => TypeID::Int4(true),
            "bigint" => TypeID::Int8(false),
            "bigint null" => TypeID::Int8(true),
            "real" => TypeID::Float4(false),
            "real null" => TypeID::Float4(true),
            "varchar" => TypeID::UTF8String(false),
            "varchar null" => TypeID::UTF8String(true),
            _ => panic!("Unknown type string {}", string),
        };
        id
    }
}
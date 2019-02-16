use super::*;

/// An enumeration of all possible concrete types
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Variant {
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    UTF8String,
    IPv4,
}

/* ============================================================================================== */

/// A complete description of a type
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct TypeID {
    pub variant: Variant,
    pub nullable: bool,
}

impl TypeID {
    pub fn new(variant: Variant, nullable: bool) -> Self {
        TypeID { variant, nullable }
    }

    pub fn from_string(string: String) -> TypeID {
        let lower = string.to_lowercase();
        let tokens: Vec<&str> = lower
            .split(' ')
            .collect();
        let variant_str = *tokens.get(0)
            .expect("Error: type string is empty");

        let variant = match variant_str {
            "smallint" => Variant::Int2,
            "int" => Variant::Int4,
            "bigint" | "long" => Variant::Int8,
            "real" => Variant::Float4,
            "double" => Variant::Float8,
            "varchar" => Variant::UTF8String,
            _ => panic!("Unknown type variant {}", variant_str)
        };

        let nullable = lower.contains("null");
        Self::new(variant, nullable)
    }

    pub fn size(&self) -> usize {
        match self.variant {
            Variant::Int2 => 2,
            Variant::Int4 => 4,
            Variant::Int8 => 8,
            Variant::Float4 => 4,
            Variant::Float8 => 8,
            Variant::UTF8String => 0,
            Variant::IPv4 => 4,
        }
    }

    pub fn next_size(&self, data: &[u8]) -> usize {
        if self.size() > 0 {
            self.size()
        } else {
            match self.variant {
                Variant::UTF8String => {
                    UTF8String::next_size(data)
                }
                _ => {
                    panic!("Variable size with no next_size() implementation");
                }
            }
        }
    }

    pub fn parse(&self, string: &str) -> Box<Value> {
        match self.variant {
            Variant::Int2 => {
                Box::new(Int2::parse(string))
            }
            Variant::Int4 => {
                Box::new(Int4::parse(string))
            }
            Variant::Int8 => {
                Box::new(Int8::parse(string))
            }
            Variant::Float4 => {
                Box::new(Float4::parse(string))
            }
            Variant::Float8 => {
                Box::new(Float8::parse(string))
            }
            Variant::IPv4 => {
                Box::new(IPv4::parse(string))
            }
            Variant::UTF8String => {
                Box::new(UTF8String::from(string))
            }
        }
    }

    pub fn create_null(&self) -> Box<Value> {
        if !self.nullable {
            panic!("Non-nullable version of {:?}", self.variant);
        }
        match self.variant {
            Variant::Int2 => {
                Box::new(Int2::create_null())
            }
            Variant::Int4 => {
                Box::new(Int4::create_null())
            }
            Variant::Int8 => {
                Box::new(Int8::create_null())
            }
            Variant::Float4 => {
                Box::new(Float4::create_null())
            }
            Variant::Float8 => {
                Box::new(Float8::create_null())
            }
            Variant::UTF8String => {
                Box::new(UTF8String::create_null())
            }
            _ => {
                panic!("Type {:?} cannot be null", self.variant);
            }
        }
    }

    pub fn create_zero(&self) -> Box<Numeric> {
        match self.variant {
            Variant::Int2 => {
                Box::new(Int2::new(0, self.nullable))
            }
            Variant::Int4 => {
                Box::new(Int4::new(0, self.nullable))
            }
            Variant::Int8 => {
                Box::new(Int8::new(0, self.nullable))
            }
            Variant::Float4 => {
                Box::new(Float4::new(0.0, self.nullable))
            }
            Variant::Float8 => {
                Box::new(Float8::new(0.0, self.nullable))
            }
            _ => {
                panic!("Type {:?} is not numeric", self.variant);
            }
        }
    }
}
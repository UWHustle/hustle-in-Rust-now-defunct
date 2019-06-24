use super::*;
use serde::{Serialize, Deserialize};

/// An enumeration of all possible concrete types
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum Variant {
    Int1,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    ByteString(usize, bool), // Represent (max_size, varchar)
    IPv4,
}

/* ============================================================================================== */

/// A complete description of a type
#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataType {
    pub variant: Variant,
    pub nullable: bool,
}

impl DataType {
    pub fn new(variant: Variant, nullable: bool) -> Self {
        DataType { variant, nullable }
    }

    pub fn from_str(string: &str) -> Result<DataType, String> {
        let lower = string.to_lowercase();
        let tokens: Vec<&str> = lower.split(' ').collect();
        let mut variant_str = *match tokens.get(0) {
            Some(val) => val,
            None => return Err(String::from("empty typename string")),
        };

        let mut size_arg: usize = 0;
        let l_paren = variant_str.find('(');
        let r_paren = variant_str.find(')');
        if l_paren != None && r_paren != None {
            let i_l = l_paren.unwrap();
            let i_r = r_paren.unwrap();
            size_arg = match variant_str[i_l + 1..i_r].parse::<usize>() {
                Ok(size) => size,
                Err(err) => return Err(err.to_string()),
            };
            variant_str = &variant_str[0..i_l];
        }

        let variant = match variant_str {
            "tinyint" => Variant::Int1,
            "smallint" => Variant::Int2,
            "int" => Variant::Int4,
            "bigint" | "long" => Variant::Int8,
            "real" | "float" => Variant::Float4,
            "double" => Variant::Float8,
            "varchar" => Variant::ByteString(size_arg, true),
            "char" => Variant::ByteString(size_arg, false),
            _ => {
                return Err(String::from(format!(
                    "unknown type variant {}",
                    variant_str
                )))
            }
        };

        let nullable = lower.contains("null");
        Ok(Self::new(variant, nullable))
    }

    pub fn to_string(&self) -> String {
        match self.variant {
            Variant::Int1 => String::from("tinyint"),
            Variant::Int2 => String::from("smallint"),
            Variant::Int4 => String::from("int"),
            Variant::Int8 => String::from("bigint"),
            Variant::Float4 => String::from("real"),
            Variant::Float8 => String::from("double"),
            Variant::ByteString(max_size, varchar) => {
                if varchar {
                    String::from(format!("varchar({})", max_size))
                } else {
                    String::from(format!("char({})", max_size))
                }
            }
            Variant::IPv4 => String::from("ipv4"),
        }
    }

    pub fn size(&self) -> usize {
        match self.variant {
            Variant::Int1 => 1,
            Variant::Int2 => 2,
            Variant::Int4 => 4,
            Variant::Int8 => 8,
            Variant::Float4 => 4,
            Variant::Float8 => 8,
            Variant::ByteString(max_size, _) => max_size,
            Variant::IPv4 => 4,
        }
    }

    pub fn next_size(&self, _data: &[u8]) -> usize {
        if self.size() > 0 {
            self.size()
        } else {
            match self.variant {
                _ => {
                    panic!("Variable size with no next_size() implementation");
                }
            }
        }
    }

    pub fn parse(&self, string: &str) -> Result<Box<Value>, String> {
        match self.variant {
            Variant::Int1 => Ok(Box::new(Int1::parse(string)?)),
            Variant::Int2 => Ok(Box::new(Int2::parse(string)?)),
            Variant::Int4 => Ok(Box::new(Int4::parse(string)?)),
            Variant::Int8 => Ok(Box::new(Int8::parse(string)?)),
            Variant::Float4 => Ok(Box::new(Float4::parse(string)?)),
            Variant::Float8 => Ok(Box::new(Float8::parse(string)?)),
            Variant::IPv4 => Ok(Box::new(IPv4::parse(string)?)),
            Variant::ByteString(max_size, varchar) => Ok(Box::new(ByteString::new(
                string.as_bytes(),
                true,
                max_size,
                varchar,
            ))),
        }
    }

    pub fn create_null(&self) -> Box<Value> {
        if !self.nullable {
            panic!("Non-nullable version of {:?}", self.variant);
        }
        match self.variant {
            Variant::Int1 => Box::new(Int1::create_null()),
            Variant::Int2 => Box::new(Int2::create_null()),
            Variant::Int4 => Box::new(Int4::create_null()),
            Variant::Int8 => Box::new(Int8::create_null()),
            Variant::Float4 => Box::new(Float4::create_null()),
            Variant::Float8 => Box::new(Float8::create_null()),
            Variant::ByteString(max_size, varchar) => {
                Box::new(ByteString::create_null(max_size, varchar))
            }
            _ => panic!("Type {:?} cannot be null", self.variant),
        }
    }

    pub fn create_zero(&self) -> Box<Numeric> {
        match self.variant {
            Variant::Int1 => Box::new(Int1::new(0, self.nullable)),
            Variant::Int2 => Box::new(Int2::new(0, self.nullable)),
            Variant::Int4 => Box::new(Int4::new(0, self.nullable)),
            Variant::Int8 => Box::new(Int8::new(0, self.nullable)),
            Variant::Float4 => Box::new(Float4::new(0.0, self.nullable)),
            Variant::Float8 => Box::new(Float8::new(0.0, self.nullable)),
            _ => panic!("Type {:?} is not numeric", self.variant),
        }
    }
}
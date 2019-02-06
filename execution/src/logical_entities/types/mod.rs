pub mod borrowed_buffer;
pub mod float;
pub mod integer;
pub mod ip_address;
pub mod owned_buffer;
pub mod utf8_string;

// These 'use' statements have the same visibility as other private code in this module (they can be
// made public using the 'pub' keyword)
use std::any::Any;

use self::float::*;
use self::integer::*;
use self::ip_address::*;
use self::owned_buffer::OwnedBuffer;
use self::utf8_string::UTF8String;

// All possible concrete types
#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub enum TypeID {
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    UTF8String,
    IPv4,
}

impl TypeID {
    // Returns the size of an object of this type; -1 if the size is variable
    pub fn size(&self) -> usize {
        match self {
            TypeID::Int2 => 2,
            TypeID::Int4 => 4,
            TypeID::Int8 => 8,
            TypeID::Float4 => 4,
            TypeID::Float8 => 8,
            TypeID::UTF8String => 0,
            TypeID::IPv4 => 4,
        }
    }

    // TODO: Generalize using pattern matching
    pub fn from_string(string: &str) -> TypeID {
        let string = string.to_lowercase().as_str();
        match string {
            "smallint" => TypeID::Int2,
            "int" => TypeID::Int4,
            "bigint" => TypeID::Int8,
            "real" => TypeID::Float4,
            "varchar" => TypeID::UTF8String,
            _ => panic!("Unknown type string {}", string),
        }
    }
}

// Types whose values are stored in a byte buffer somewhere
pub trait BufferType {
    fn type_id(&self) -> TypeID;

    fn data(&self) -> &[u8];

    fn marshall(&self) -> Box<ValueType> {
        match self.type_id() {
            TypeID::Int2 => {
                Box::new(Int2::marshall(self.data()))
            }
            TypeID::Int4 => {
                Box::new(Int4::marshall(self.data()))
            }
            TypeID::Int8 => {
                Box::new(Int8::marshall(self.data()))
            }
            TypeID::Float4 => {
                Box::new(Float4::marshall(self.data()))
            }
            TypeID::Float8 => {
                Box::new(Float8::marshall(self.data()))
            }
            TypeID::UTF8String => {
                Box::new(UTF8String::marshall(self.data()))
            }
            TypeID::IPv4 => {
                Box::new(IPv4::marshall(self.data()))
            }
        }
    }
}

pub trait Numeric: ValueType {
    fn zero(type_id: TypeID) -> Box<Numeric> {
        match type_id {
            TypeID::Int2 => {
                Box::new(Int2::new(0))
            }
            TypeID::Int4 => {
                Box::new(Int4::new(0))
            }
            TypeID::Int8 => {
                Box::new(Int8::new(0))
            }
            TypeID::Float4 => {
                Box::new(Float4::new(0.0))
            }
            TypeID::Float8 => {
                Box::new(Float8::new(0.0))
            }
            _ => panic!("Type {:?} is not numeric", type_id)
        }
    }

    fn add(&self, other: &Numeric) -> Box<Numeric>;

    fn divide(&self, other: &Numeric) -> Box<Float>;
}

// Values are stored as various types - concrete implementations can define a 'value()' method which
// returns the internal type
pub trait ValueType: Castable + Any {
    fn un_marshall(&self) -> OwnedBuffer;

    // This should be overriden for types which don't have a constant size (i.e. strings)
    fn size(&self) -> usize {
        self.type_id().size()
    }
    fn type_id(&self) -> TypeID;

    fn compare(&self, other: &ValueType, cmp: Comparator) -> bool;

    fn equals(&self, other: &ValueType) -> bool {
        self.compare(other, Comparator::Equal)
    }

    fn less(&self, other: &ValueType) -> bool {
        self.compare(other, Comparator::Less)
    }

    fn less_eq(&self, other: &ValueType) -> bool {
        self.compare(other, Comparator::Less) || self.compare(other, Comparator::Equal)
    }

    fn greater(&self, other: &ValueType) -> bool {
        self.compare(other, Comparator::Greater)
    }

    fn greater_eq(&self, other: &ValueType) -> bool {
        self.compare(other, Comparator::Greater) || self.compare(other, Comparator::Equal)
    }

    fn add(&self, other: &ValueType) -> Box<ValueType>;
}

// Used to allow downcasting
pub trait Castable {
    fn as_any(&self) -> &Any;
}

impl<T: ValueType> Castable for T {
    fn as_any(&self) -> &Any {
        self
    }
}

fn cast<T: ValueType>(value: &ValueType) -> &T {
    value.as_any().downcast_ref::<T>().expect("Casting failed")
}

// Useful in preventing comparison code duplication
pub enum Comparator {
    Less,
    Equal,
    Greater,
}

impl Comparator {
    fn apply<T: PartialOrd>(&self, val1: T, val2: T) -> bool {
        match self {
            Comparator::Less => {
                val1 < val2
            }
            Comparator::Equal => {
                val1 == val2
            }
            Comparator::Greater => {
                val1 > val2
            }
        }
    }
}
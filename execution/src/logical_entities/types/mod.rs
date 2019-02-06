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
        }
    }

    pub fn create_null(&self) -> Box<ValueType> {
        if !self.nullable() {
            panic!("Trying to create NULL with non-nullable variant of {:?}");
        }
        match self {
            TypeID::Int2() => {
                Box::new(Int2::create_null())
            }
            TypeID::Int4() => {
                Box::new(Int4::create_null())
            }
            TypeID::Int8() => {
                Box::new(Int8::create_null())
            }
            TypeID::Float4() => {
                Box::new(Float4::create_null())
            }
            TypeID::Float8() => {
                Box::new(Float8::create_null())
            }
            TypeID::UTF8String() => {
                Box::new(UTF8String::create_null())
            }
            _ => panic!("Type {:?} cannot be NULL", self)
        }
    }

    pub fn from_string(string: &str) -> TypeID {
        let string = string.to_lowercase().as_str();
        match string {
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
        }
    }
}

// Types whose values are stored in a byte buffer somewhere
pub trait BufferType {
    fn type_id(&self) -> TypeID;

    fn data(&self) -> &[u8];

    fn marshall(&self) -> Box<ValueType> {
        match self.type_id() {
            TypeID::Int2() => {
                Box::new(Int2::marshall(self.data()))
            }
            TypeID::Int4() => {
                Box::new(Int4::marshall(self.data()))
            }
            TypeID::Int8() => {
                Box::new(Int8::marshall(self.data()))
            }
            TypeID::Float4() => {
                Box::new(Float4::marshall(self.data()))
            }
            TypeID::Float8() => {
                Box::new(Float8::marshall(self.data()))
            }
            TypeID::UTF8String() => {
                Box::new(UTF8String::marshall(self.data()))
            }
            TypeID::IPv4() => {
                Box::new(IPv4::marshall(self.data()))
            }
        }
    }
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

    fn is_null(&self) -> bool;

    fn to_string(&self) -> &str;
}

pub trait Numeric: ValueType {
    fn zero(type_id: TypeID) -> Box<Numeric> {
        match type_id {
            TypeID::Int2() => {
                Box::new(Int2::new(0))
            }
            TypeID::Int4() => {
                Box::new(Int4::new(0))
            }
            TypeID::Int8() => {
                Box::new(Int8::new(0))
            }
            TypeID::Float4() => {
                Box::new(Float4::new(0.0))
            }
            TypeID::Float8() => {
                Box::new(Float8::new(0.0))
            }
            _ => panic!("Type {:?} is not numeric", type_id)
        }
    }

    fn add(&self, other: &Numeric) -> Box<Numeric>;

    fn divide(&self, other: &Numeric) -> Box<Float>;
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
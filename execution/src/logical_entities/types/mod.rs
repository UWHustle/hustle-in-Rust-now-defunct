pub mod borrowed_buffer;
pub mod float;
pub mod integer;
pub mod ip_address;
pub mod owned_buffer;
pub mod utf8_string;

// These 'use' statements have the same visibility as other private code in this module (they can be
// made public using the 'pub' keyword)
use std::ops::*;
use std::any::Any;
use std::fmt::Debug;

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

// Types whose values are stored in a byte buffer somewhere
pub trait BufferType {
    fn type_id(&self) -> TypeID;

    fn data(&self) -> &[u8];

    fn is_null(&self) -> bool;

    fn marshall(&self) -> Box<ValueType> {
        match self.type_id() {
            TypeID::Int2(nullable) => {
                Box::new(Int2::marshall(nullable, self.is_null(), self.data()))
            }
            TypeID::Int4(nullable) => {
                Box::new(Int4::marshall(nullable, self.is_null(), self.data()))
            }
            TypeID::Int8(nullable) => {
                Box::new(Int8::marshall(nullable, self.is_null(), self.data()))
            }
            TypeID::Float4(nullable) => {
                Box::new(Float4::marshall(nullable, self.is_null(), self.data()))
            }
            TypeID::Float8(nullable) => {
                Box::new(Float8::marshall(nullable, self.is_null(), self.data()))
            }
            TypeID::UTF8String(nullable) => {
                Box::new(UTF8String::marshall(nullable, self.is_null(), self.data()))
            }
            TypeID::IPv4(nullable) => {
                Box::new(IPv4::marshall(nullable, self.is_null(), self.data()))
            }
        }
    }
}

// Values are stored as various types - concrete implementations can define a 'value()' method which
// returns the internal type
pub trait ValueType: Castable + Any + BoxClone + Debug {
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

    fn to_string(&self) -> String;
}

pub trait Numeric: ValueType + ValueTypeUpCast + BoxNumericClone {
    fn arithmetic(&self, other: &Numeric, oper: Arithmetic) -> Box<Numeric>;

    fn add(&self, other: &Numeric) -> Box<Numeric> {
        self.arithmetic(other, Arithmetic::Add)
    }

    fn divide(&self, other: &Numeric) -> Box<Numeric> {
        self.arithmetic(other, Arithmetic::Divide)
    }
}

pub struct BoxedValue {
    value: Box<ValueType>
}

impl BoxedValue {
    pub fn new(value: Box<ValueType>) -> Self {
        Self { value }
    }
    pub fn value(&self) -> &ValueType {
        &*self.value
    }
}

impl Clone for BoxedValue {
    fn clone(&self) -> BoxedValue {
        BoxedValue { value: self.value.box_clone() }
    }
}

pub struct BoxedNumeric {
    value: Box<Numeric>
}

impl BoxedNumeric {
    pub fn new(value: Box<Numeric>) -> Self {
        Self { value }
    }
    pub fn value(&self) -> &Numeric { // TODO: Use operator overloading
        &*self.value
    }
}

impl Clone for Box<ValueType> {
    fn clone(&self) -> Box<ValueType> {
        self.box_clone()
    }
}

impl Clone for Box<Numeric> {
    fn clone(&self) -> Box<Numeric> {
        self.box_numeric_clone()
    }
}

impl Clone for BoxedNumeric {
    fn clone(&self) -> BoxedNumeric {
        BoxedNumeric { value: self.value.box_numeric_clone() }
    }
}

pub trait BoxNumericClone {
    fn box_numeric_clone(&self) -> Box<Numeric>;
}

impl<T: Numeric + Clone> BoxNumericClone for T {
    fn box_numeric_clone(&self) -> Box<Numeric> {
        Box::new(self.clone())
    }
}

// Allows for cloning of trait objects
pub trait BoxClone {
    fn box_clone(&self) -> Box<ValueType>; // TODO: Possibly make this a generic type?
}

impl<T: ValueType + Clone> BoxClone for T {
    fn box_clone(&self) -> Box<ValueType> {
        Box::new(self.clone())
    }
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

pub trait ValueTypeUpCast {
    fn as_value_type(&self) -> &ValueType;
}

impl<T: Numeric> ValueTypeUpCast for T {
    fn as_value_type(&self) -> &ValueType {
        self
    }
}

pub fn force_numeric(value: &ValueType) -> &Numeric {
    match value.type_id() {
        TypeID::Int2(nullable) => {
            cross_cast::<Int2>(value)
        }
        TypeID::Int4(nullable) => {
            cross_cast::<Int4>(value)
        }
        TypeID::Int8(nullable) => {
            cross_cast::<Int8>(value)
        }
        TypeID::Float4(nullable) => {
            cross_cast::<Float4>(value)
        }
        TypeID::Float8(nullable) => {
            cross_cast::<Float8>(value)
        }
        _ => panic!("{:?} is not a numeric type", value.type_id())
    }
}

fn value_cast<T: ValueType>(value: &ValueType) -> &T {
    value.as_any().downcast_ref::<T>().expect("Casting failed")
}

fn cross_cast<T: Numeric>(value: &ValueType) -> &T {
    value.as_any().downcast_ref::<T>().expect("Casting failed")
}

fn numeric_cast<T: Numeric>(value: &Numeric) -> &T {
    value.as_any().downcast_ref::<T>().expect("Casting failed")
}

// Useful in preventing comparison code duplication
#[derive(Clone)]
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

// Useful in preventing arithmetic code duplication
pub enum Arithmetic {
    Add,
    Divide,
}

impl Arithmetic {
    fn apply<T: Add<Output=T> + Div<Output=T>>(&self, val1: T, val2: T) -> T {
        match self {
            Arithmetic::Add => {
                val1 + val2
            }
            Arithmetic::Divide => {
                val1 / val2
            }
        }
    }
}
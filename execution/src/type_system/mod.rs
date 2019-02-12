pub mod borrowed_buffer;
pub mod float;
pub mod integer;
pub mod ip_address;
pub mod owned_buffer;
pub mod type_id;
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
use self::type_id::*;

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
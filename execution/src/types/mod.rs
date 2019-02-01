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
#[derive(Debug, Clone)]
pub enum TypeID {
    Int2,
    Int4,
    Int8,
    Float8,
    UTF8String,
    IPv4,
}

// Types whose values are stored in a byte buffer somewhere
pub trait BufferType {
    fn type_id(&self) -> TypeID;
    fn data(&self) -> &[u8];

    fn marshall(&self) -> Box<ValueType> {
        match self.type_id() {
            TypeID::Int2 => {
                Box::new(Int2::new(self.data()))
            }
            TypeID::Int4 => {
                Box::new(Int4::new(self.data()))
            }
            TypeID::Int8 => {
                Box::new(Int8::new(self.data()))
            }
            TypeID::Float8 => {
                Box::new(Float8::new(self.data()))
            }
            TypeID::UTF8String => {
                Box::new(UTF8String::new(self.data()))
            }
            TypeID::IPv4 => {
                Box::new(IPv4::new(self.data()))
            }
        }
    }
}

// Values are stored as various types - concrete implementations can define a 'value()' method which
// returns the internal type
pub trait ValueType: Castable + Any {
    // TODO: Require ValueType to implement PartialOrd<ValueType>
    fn un_marshall(&self) -> OwnedBuffer;
    fn size(&self) -> usize;
    fn type_id(&self) -> TypeID;
    fn equals(&self, other: &ValueType) -> bool;
    // TODO: Use lambda to reduce duplicate code
    fn less_than(&self, other: &ValueType) -> bool;
    fn greater_than(&self, other: &ValueType) -> bool;
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
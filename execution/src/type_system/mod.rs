pub mod borrowed_buffer;
pub mod float;
pub mod integer;
pub mod ip_address;
pub mod operators;
pub mod owned_buffer;
pub mod type_id;
pub mod utf8_string;

use std::any::Any;
use std::fmt::Debug;

use self::float::*;
use self::integer::*;
use self::ip_address::*;
use self::operators::*;
use self::owned_buffer::*;
use self::type_id::*;
use self::utf8_string::*;

/* ============================================================================================== */

/// Types whose values are stored in a byte buffer somewhere
pub trait Buffer {
    fn type_id(&self) -> TypeID;
    fn is_null(&self) -> bool;
    fn data(&self) -> &[u8];

    /// Converts the bytes in the buffer to a `Value` which is stored on the heap
    fn marshall(&self) -> Box<Value> {
        let nullable = self.type_id().nullable;
        let is_null = self.is_null();
        let data = self.data();
        match self.type_id().variant {
            Variant::Int2 => {
                Box::new(Int2::marshall(nullable, is_null, data))
            }
            Variant::Int4 => {
                Box::new(Int4::marshall(nullable, is_null, data))
            }
            Variant::Int8 => {
                Box::new(Int8::marshall(nullable, is_null, data))
            }
            Variant::Float4 => {
                Box::new(Float4::marshall(nullable, is_null, data))
            }
            Variant::Float8 => {
                Box::new(Float8::marshall(nullable, is_null, data))
            }
            Variant::UTF8String => {
                Box::new(UTF8String::marshall(nullable, is_null, data))
            }
            Variant::IPv4 => {
                Box::new(IPv4::marshall(nullable, is_null, data))
            }
        }
    }
}

/* ============================================================================================== */

/// Values are stored as various types - concrete implementations can define a 'value()' method
/// which returns the internal type
pub trait Value: Any + AsAny + AsValue + BoxCloneValue + Debug {
    fn type_id(&self) -> TypeID;
    fn is_null(&self) -> bool;
    fn to_string(&self) -> String;

    /// Converts the value to an `OwnedBuffer` (essentially a vector of bytes)
    fn un_marshall(&self) -> OwnedBuffer;

    /// Applies the specified comparator to compare `self` with `other`
    fn compare(&self, other: &Value, cmp: Comparator) -> bool;

    /// By default returns self.type_id().size()
    /// Should be overriden for types which don't have a constant size (i.e. strings)
    fn size(&self) -> usize {
        self.type_id().size()
    }

    fn equals(&self, other: &Value) -> bool {
        self.compare(other, Comparator::Equal)
    }

    fn less(&self, other: &Value) -> bool {
        self.compare(other, Comparator::Less)
    }

    fn less_eq(&self, other: &Value) -> bool {
        self.compare(other, Comparator::Less) || self.compare(other, Comparator::Equal)
    }

    fn greater(&self, other: &Value) -> bool {
        self.compare(other, Comparator::Greater)
    }

    fn greater_eq(&self, other: &Value) -> bool {
        self.compare(other, Comparator::Greater) || self.compare(other, Comparator::Equal)
    }
}

/// Allows for upcasting to `Any`
pub trait AsAny {
    fn as_any(&self) -> &Any;
}

impl<T: Any> AsAny for T {
    fn as_any(&self) -> &Any {
        self
    }
}

/// Allows for upcasting to `Value`
pub trait AsValue {
    fn as_value(&self) -> &Value;
}

impl<T: Value> AsValue for T {
    fn as_value(&self) -> &Value {
        self
    }
}

/// Allows `Value` objects to be cloned to the heap
pub trait BoxCloneValue {
    fn box_clone_value(&self) -> Box<Value>;
}

impl<T: Value + Clone> BoxCloneValue for T {
    fn box_clone_value(&self) -> Box<Value> {
        Box::new(self.clone())
    }
}

/// Ensures that `Box<Value>` is cloneable even though `Value` isn't
impl Clone for Box<Value> {
    fn clone(&self) -> Self {
        self.box_clone_value()
    }
}

/// Allows for downcasting of `Value` trait objects
pub fn cast_value<T: Value>(value: &Value) -> &T {
    value.as_any().downcast_ref::<T>().expect("Casting failed")
}

/// Helper for "incomparable types" message
fn incomparable(type_1: TypeID, type_2: TypeID) -> String {
    format!("Unable to compare type {:?} to type {:?}", type_1.variant, type_2.variant)
}

/// Helper for "null value" message
fn null_value(type_id: TypeID) -> String {
    format!("Attempting to retrieve value of null {:?}", type_id.variant)
}

/* ============================================================================================== */

/// Values on which arithmetic operations can be performed
pub trait Numeric: Value + AsNumeric + BoxCloneNumeric {
    fn arithmetic(&self, other: &Numeric, oper: Arithmetic) -> Box<Numeric>;

    fn add(&self, other: &Numeric) -> Box<Numeric> {
        self.arithmetic(other, Arithmetic::Add)
    }

    fn subtract(&self, other: &Numeric) -> Box<Numeric> {
        self.arithmetic(other, Arithmetic::Subtract)
    }

    fn multiply(&self, other: &Numeric) -> Box<Numeric> {
        self.arithmetic(other, Arithmetic::Multiply)
    }

    fn divide(&self, other: &Numeric) -> Box<Numeric> {
        self.arithmetic(other, Arithmetic::Divide)
    }
}

/// Allows for upcasting to `Numeric`
pub trait AsNumeric {
    fn as_numeric(&self) -> &Numeric;
}

impl<T: Numeric> AsNumeric for T {
    fn as_numeric(&self) -> &Numeric {
        self
    }
}

/// Allows `Numeric` objects to be cloned to the heap
pub trait BoxCloneNumeric {
    fn box_clone_numeric(&self) -> Box<Numeric>;
}

impl<T: Numeric + Clone> BoxCloneNumeric for T {
    fn box_clone_numeric(&self) -> Box<Numeric> {
        Box::new(self.clone())
    }
}

/// Ensures that `Box<Numeric>` is cloneable even though `Numeric` isn't
impl Clone for Box<Numeric> {
    fn clone(&self) -> Self {
        self.box_clone_numeric()
    }
}

/// Allows for downcasting of `Numeric` trait objects
pub fn cast_numeric<T: Numeric>(value: &Numeric) -> &T {
    value.as_any().downcast_ref::<T>().expect("Casting failed")
}

/// Forces a `Value` to be interpreted as a `Numeric`
///
/// Panics if the input value is not of a numeric type
pub fn force_numeric(value: &Value) -> &Numeric {
    match value.type_id().variant {
        Variant::Int2 => {
            cast_value::<Int2>(value)
        }
        Variant::Int4 => {
            cast_value::<Int4>(value)
        }
        Variant::Int8 => {
            cast_value::<Int8>(value)
        }
        Variant::Float4 => {
            cast_value::<Float4>(value)
        }
        Variant::Float8 => {
            cast_value::<Float8>(value)
        }
        _ => panic!("{:?} is not a numeric type", value.type_id())
    }
}

/// Helper for "not numeric" message
fn not_numeric(type_id: TypeID) -> String {
    format!("Type {:?} is not numeric", type_id.variant)
}
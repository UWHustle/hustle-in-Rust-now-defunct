#[macro_use]
extern crate serde;

use std::mem::size_of;
use std::ops::DerefMut;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TypeVariant {
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TypeInfo {
    pub variant: TypeVariant,
    pub size: usize,
}

pub trait HustleType<D: DerefMut<Target = [u8]>> {
    fn interpret(buf: D) -> Self;
    fn get_info(&self) -> &TypeInfo;
}

pub trait PrimitiveType<D: DerefMut<Target = [u8]>> {
    type Primitive;
    fn from_primitive(n: Self::Primitive, buf: D) -> Self;
    fn get_primitive(&self) -> Self::Primitive;
}

pub trait Comparable<T> {
    fn eq(&self, other: &T) -> bool;
    fn lt(&self, other: &T) -> bool;
    fn le(&self, other: &T) -> bool;
    fn gt(&self, other: &T) -> bool;
    fn ge(&self, other: &T) -> bool;
}

macro_rules! make_primitive {
    ($name:ident, $variant:expr, $primitive_ty:ty) => {
        pub struct $name<D: DerefMut<Target = [u8]>> {
            info: TypeInfo,
            ptr: *mut $primitive_ty,
            _buf: D,
        }

        impl<D: DerefMut<Target = [u8]>> HustleType<D> for $name<D> {
            fn interpret(mut buf: D) -> Self {
                assert_eq!(
                    buf.len(),
                    size_of::<$primitive_ty>(),
                    "Incorrectly sized buffer for type {:?}",
                    $variant,
                );
                let info = TypeInfo { variant: $variant, size: size_of::<$primitive_ty>() };
                let ptr = buf.as_mut_ptr() as *mut $primitive_ty;
                $name {
                    info,
                    ptr,
                    _buf: buf,
                }
            }

            fn get_info(&self) -> &TypeInfo {
                &self.info
            }
        }

        impl<D: DerefMut<Target = [u8]>> PrimitiveType<D> for $name<D> {
            type Primitive = $primitive_ty;

            fn from_primitive(n: $primitive_ty, buf: D) -> Self {
                let t = Self::interpret(buf);
                unsafe { *t.ptr = n };
                t
            }

            fn get_primitive(&self) -> $primitive_ty {
                unsafe { *self.ptr }
            }
        }
    };
}

macro_rules! make_primitive_comparable {
    ($left:ident, $right:ident, $cast_ty:ty) => {
        impl<L, R> Comparable<$right<R>> for $left<L>
        where
            L: DerefMut<Target = [u8]>,
            R: DerefMut<Target = [u8]>,
        {
            fn eq(&self, other: &$right<R>) -> bool {
                (self.get_primitive() as $cast_ty) == (other.get_primitive() as $cast_ty)
            }

            fn lt(&self, other: &$right<R>) -> bool {
                (self.get_primitive() as $cast_ty) < (other.get_primitive() as $cast_ty)
            }

            fn le(&self, other: &$right<R>) -> bool {
                (self.get_primitive() as $cast_ty) <= (other.get_primitive() as $cast_ty)
            }

            fn gt(&self, other: &$right<R>) -> bool {
                (self.get_primitive() as $cast_ty) > (other.get_primitive() as $cast_ty)
            }

            fn ge(&self, other: &$right<R>) -> bool {
                (self.get_primitive() as $cast_ty) >= (other.get_primitive() as $cast_ty)
            }
        }
    };
}

make_primitive!(Int8, TypeVariant::Int8, i8);
make_primitive!(Int16, TypeVariant::Int16, i16);
make_primitive!(Int32, TypeVariant::Int32, i32);
make_primitive!(Int64, TypeVariant::Int64, i64);
make_primitive!(Float32, TypeVariant::Float32, f32);
make_primitive!(Float64, TypeVariant::Float64, f64);

make_primitive_comparable!(Int8, Int8, i8);
make_primitive_comparable!(Int8, Int16, i16);
make_primitive_comparable!(Int8, Int32, i32);
make_primitive_comparable!(Int8, Int64, i64);
make_primitive_comparable!(Int8, Float32, f32);
make_primitive_comparable!(Int8, Float64, f64);

make_primitive_comparable!(Int16, Int8, i16);
make_primitive_comparable!(Int16, Int16, i16);
make_primitive_comparable!(Int16, Int32, i32);
make_primitive_comparable!(Int16, Int64, i64);
make_primitive_comparable!(Int16, Float32, f32);
make_primitive_comparable!(Int16, Float64, f64);

make_primitive_comparable!(Int32, Int8, i32);
make_primitive_comparable!(Int32, Int16, i32);
make_primitive_comparable!(Int32, Int32, i32);
make_primitive_comparable!(Int32, Int64, i64);
make_primitive_comparable!(Int32, Float32, f64);
make_primitive_comparable!(Int32, Float64, f64);

make_primitive_comparable!(Int64, Int8, i64);
make_primitive_comparable!(Int64, Int16, i64);
make_primitive_comparable!(Int64, Int32, i64);
make_primitive_comparable!(Int64, Int64, i64);
//make_comparable!(Int64, Float32, f64);
//make_comparable!(Int64, Float64, f64);

make_primitive_comparable!(Float32, Int8, f32);
make_primitive_comparable!(Float32, Int16, f32);
make_primitive_comparable!(Float32, Int32, f64);
//make_comparable!(Float32, Int64, f64);
make_primitive_comparable!(Float32, Float32, f32);
make_primitive_comparable!(Float32, Float64, f64);

make_primitive_comparable!(Float64, Int8, f64);
make_primitive_comparable!(Float64, Int16, f64);
make_primitive_comparable!(Float64, Int32, f64);
//make_comparable!(Float64, Int64, f64);
make_primitive_comparable!(Float64, Float32, f64);
make_primitive_comparable!(Float64, Float64, f64);

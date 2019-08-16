use std::mem::size_of;
use std::ops::DerefMut;

#[derive(Debug)]
pub enum HustleTypeVariant {
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
}

pub trait HustleType {
    fn variant(&self) -> HustleTypeVariant;
    fn size(&self) -> usize;
}

pub trait Comparable<T> {
    fn eq(&self, other: &T) -> bool;
    fn lt(&self, other: &T) -> bool;
    fn le(&self, other: &T) -> bool;
    fn gt(&self, other: &T) -> bool;
    fn ge(&self, other: &T) -> bool;
}

macro_rules! make_type {
    ($name:ident, $variant:expr, $primitive_ty:ty) => {
        pub struct $name<D: DerefMut<Target = [u8]>> {
            ptr: *mut $primitive_ty,
            _buf: D,
        }

        impl $name<Vec<u8>> {
            pub fn new(n: $primitive_ty) -> Self {
                let buf = vec![0; size_of::<$primitive_ty>()];
                let t = Self::interpret(buf);
                unsafe { *t.ptr = n };
                t
            }
        }

        impl<D: DerefMut<Target = [u8]>> $name<D> {
            pub fn interpret(mut buf: D) -> Self {
                assert_eq!(
                    buf.len(),
                    size_of::<$primitive_ty>(),
                    "Incorrectly sized buffer for type {:?}",
                    $variant,
                );
                let ptr = buf.as_mut_ptr() as *mut $primitive_ty;
                $name {
                    ptr,
                    _buf: buf,
                }
            }

            pub fn get_primitive(&self) -> $primitive_ty {
                unsafe { *self.ptr }
            }
        }

        impl<D: DerefMut<Target = [u8]>> HustleType for $name<D> {
            fn variant(&self) -> HustleTypeVariant {
                $variant
            }

            fn size(&self) -> usize {
                size_of::<$primitive_ty>()
            }
        }

    };
}

macro_rules! make_comparable {
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

make_type!(Int8, HustleTypeVariant::Int8, i8);
make_type!(Int16, HustleTypeVariant::Int16, i16);
make_type!(Int32, HustleTypeVariant::Int32, i32);
make_type!(Int64, HustleTypeVariant::Int64, i64);
make_type!(Float32, HustleTypeVariant::Float32, f32);
make_type!(Float64, HustleTypeVariant::Float64, f64);

make_comparable!(Int8, Int8, i8);
make_comparable!(Int8, Int16, i16);
make_comparable!(Int8, Int32, i32);
make_comparable!(Int8, Int64, i64);
make_comparable!(Int8, Float32, f32);
make_comparable!(Int8, Float64, f64);

make_comparable!(Int16, Int8, i16);
make_comparable!(Int16, Int16, i16);
make_comparable!(Int16, Int32, i32);
make_comparable!(Int16, Int64, i64);
make_comparable!(Int16, Float32, f32);
make_comparable!(Int16, Float64, f64);

make_comparable!(Int32, Int8, i32);
make_comparable!(Int32, Int16, i32);
make_comparable!(Int32, Int32, i32);
make_comparable!(Int32, Int64, i64);
make_comparable!(Int32, Float32, f64);
make_comparable!(Int32, Float64, f64);

make_comparable!(Int64, Int8, i64);
make_comparable!(Int64, Int16, i64);
make_comparable!(Int64, Int32, i64);
make_comparable!(Int64, Int64, i64);
//make_comparable!(Int64, Float32, f64);
//make_comparable!(Int64, Float64, f64);

make_comparable!(Float32, Int8, f32);
make_comparable!(Float32, Int16, f32);
make_comparable!(Float32, Int32, f64);
//make_comparable!(Float32, Int64, f64);
make_comparable!(Float32, Float32, f32);
make_comparable!(Float32, Float64, f64);

make_comparable!(Float64, Int8, f64);
make_comparable!(Float64, Int16, f64);
make_comparable!(Float64, Int32, f64);
//make_comparable!(Float64, Int64, f64);
make_comparable!(Float64, Float32, f64);
make_comparable!(Float64, Float64, f64);

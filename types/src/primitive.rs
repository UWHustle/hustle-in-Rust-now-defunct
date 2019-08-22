use std::mem::size_of;

use crate::{CompareEq, CompareOrd, HustleType};

macro_rules! make_primitive_type {
    ($name:ident, $primitive_ty:ty) => {
        #[derive(Serialize, Deserialize)]
        pub struct $name;

        impl $name {
            pub fn get(&self, buf: &[u8]) -> $primitive_ty {
                unsafe { *(buf.as_ptr() as *const $primitive_ty) }
            }

            pub fn set(&self, val: $primitive_ty, buf: &[u8]) {
                unsafe { *(buf.as_ptr() as *mut $primitive_ty) = val }
            }
        }

        impl HustleType for $name {
            fn byte_len(&self) -> usize {
                size_of::<$primitive_ty>()
            }
        }
    };
}

macro_rules! make_compare {
    ($impl_ty:ty, $compare_ident:ident, $as_ty:ty) => {
        impl CompareEq<$compare_ident> for $impl_ty {
            fn eq(&self, _other: &$compare_ident, left: &[u8], right: &[u8]) -> bool {
                unsafe { *(left.as_ptr() as *const $as_ty) == *(right.as_ptr() as *const $as_ty) }
            }
        }

        impl CompareOrd<$compare_ident> for $impl_ty {
            fn lt(&self, _other: &$compare_ident, left: &[u8], right: &[u8]) -> bool {
                unsafe { *(left.as_ptr() as *const $as_ty) < *(right.as_ptr() as *const $as_ty) }
            }

            fn le(&self, _other: &$compare_ident, left: &[u8], right: &[u8]) -> bool {
                unsafe { *(left.as_ptr() as *const $as_ty) <= *(right.as_ptr() as *const $as_ty) }
            }

            fn gt(&self, _other: &$compare_ident, left: &[u8], right: &[u8]) -> bool {
                unsafe { *(left.as_ptr() as *const $as_ty) > *(right.as_ptr() as *const $as_ty) }
            }

            fn ge(&self, _other: &$compare_ident, left: &[u8], right: &[u8]) -> bool {
                unsafe { *(left.as_ptr() as *const $as_ty) >= *(right.as_ptr() as *const $as_ty) }
            }
        }
    };
}

make_primitive_type!(Bool, bool);
make_primitive_type!(Int8, i8);
make_primitive_type!(Int16, i16);
make_primitive_type!(Int32, i32);
make_primitive_type!(Int64, i64);
make_primitive_type!(UInt8, u8);
make_primitive_type!(UInt16, u16);
make_primitive_type!(UInt32, u32);
make_primitive_type!(UInt64, u64);

make_compare!(Bool, Bool, bool);

make_compare!(Int8, Int8, i8);
make_compare!(Int8, Int16, i16);
make_compare!(Int8, Int32, i32);
make_compare!(Int8, Int64, i64);

make_compare!(Int16, Int8, i16);
make_compare!(Int16, Int16, i16);
make_compare!(Int16, Int32, i32);
make_compare!(Int16, Int64, i64);

make_compare!(Int32, Int8, i32);
make_compare!(Int32, Int16, i32);
make_compare!(Int32, Int32, i32);
make_compare!(Int32, Int64, i64);

make_compare!(Int64, Int8, i64);
make_compare!(Int64, Int16, i64);
make_compare!(Int64, Int32, i64);
make_compare!(Int64, Int64, i64);

make_compare!(UInt8, UInt8, u8);
make_compare!(UInt8, UInt16, u16);
make_compare!(UInt8, UInt32, u32);
make_compare!(UInt8, UInt64, u64);

make_compare!(UInt16, UInt8, u16);
make_compare!(UInt16, UInt16, u16);
make_compare!(UInt16, UInt32, u32);
make_compare!(UInt16, UInt64, u64);

make_compare!(UInt32, UInt8, u32);
make_compare!(UInt32, UInt16, u32);
make_compare!(UInt32, UInt32, u32);
make_compare!(UInt32, UInt64, u64);

make_compare!(UInt64, UInt8, u64);
make_compare!(UInt64, UInt16, u64);
make_compare!(UInt64, UInt32, u64);
make_compare!(UInt64, UInt64, u64);

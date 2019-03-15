extern crate sqlite;

pub mod logical_entities;
pub mod physical_operators;
pub mod physical_plan;
pub mod relational_api;
pub mod test_helpers;
pub mod type_system;

use std::os::raw::c_char;

use physical_plan::parser::parse;

#[no_mangle]
pub extern "C" fn execute_plan(name: *const c_char) {
    let plan_string = from_cstr(name);
    let node = parse(plan_string.as_str());
    node.execute();
}

use std::ffi::CStr;
use std::ffi::CString;
use std::mem::forget;

#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn from_cstr(c_str: *const c_char) -> String {
    let cstr = unsafe { CStr::from_ptr(c_str) };
    let b = cstr
        .to_str()
        .expect("Relation name not a valid UTF-8 string");
    b.to_string()
}

pub fn to_cstr(str: String) -> *const c_char {
    let a = CString::new(str).unwrap();
    let p = a.as_ptr();
    forget(a);
    p
}

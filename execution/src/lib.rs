pub mod logical_entities;
pub mod physical_operators;
pub mod physical_plan;
pub mod relational_api;
pub mod test_helpers;
pub mod type_system;

use physical_plan::global_sm;
use physical_plan::parser::parse;

use std::ffi::CStr;
use std::os::raw::c_char;

extern crate storage;

#[no_mangle]
pub extern "C" fn execute_plan(name: *const c_char) {
    let plan_string = from_cstr(name);
    let node = parse(plan_string.as_str());
    node.execute(global_sm::get());
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn from_cstr(c_str: *const c_char) -> String {
    let cstr = unsafe { CStr::from_ptr(c_str) };
    let b = cstr
        .to_str()
        .expect("Relation name not a valid UTF-8 string");
    b.to_string()
}

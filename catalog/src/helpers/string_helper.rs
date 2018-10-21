
use std::os::raw::c_char;
use std::ffi::CStr;
use std::ffi::CString;
use std::mem::forget;

pub fn from_cstr(c_str: *const c_char) -> String {
    let cstr = unsafe {
        CStr::from_ptr(c_str)
    };
    let b = cstr.to_str().expect("Relation name not a valid UTF-8 string");
    let c= b.to_string();
    c
}


pub fn to_cstr(str: String) -> *const c_char {
    let a= CString::new(str).unwrap();
    let p = a.as_ptr();
    forget(a);
    p
}
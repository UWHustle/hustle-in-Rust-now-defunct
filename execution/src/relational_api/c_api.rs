use relational_api::rust_api::ImmediateRelation;
use std::collections::hash_set::HashSet;
use std::ffi::c_void;
use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;

// TODO: Unsafe - wrap this in a mutex
static mut CURRENT_RELATIONS: *mut HashSet<Box<ImmediateRelation>> = ptr::null_mut();

fn convert_cstr<'a>(c_string: *const c_char) -> &'a str {
    unsafe {
        CStr::from_ptr(c_string)
            .to_str()
            .expect("Invalid utf8-string")
    }
}

fn convert_cstr_list<'a>(c_string_list: *const *const c_char, n_strings: isize) -> Vec<&'a str> {
    let mut output: Vec<&str> = vec![];
    for i in 0..n_strings {
        let c_string = unsafe { *c_string_list.offset(i) };
        output.push(convert_cstr(c_string));
    }
    output
}

#[no_mangle]
pub extern "C" fn new_relation(
    cstr_col_names: *const *const c_char,
    cstr_col_type_names: *const *const c_char,
    n_cols: i32,
) -> *const c_void {
    unsafe {
        if CURRENT_RELATIONS == ptr::null_mut() {
            CURRENT_RELATIONS = Box::into_raw(Box::new(HashSet::new()))
        }
    }

    let n_cols = n_cols as isize;
    let col_names = convert_cstr_list(cstr_col_names, n_cols);
    let col_type_names = convert_cstr_list(cstr_col_type_names, n_cols);

    let relation = Box::new(ImmediateRelation::new(col_names, col_type_names));
    let c_pointer = (&*relation) as *const ImmediateRelation as *const c_void;
    unsafe { (*CURRENT_RELATIONS).insert(relation) };
    c_pointer
}

#[no_mangle]
pub extern "C" fn drop_relation(relation_c_ptr: *const c_void) {
    let relation = relation_c_ptr as *const ImmediateRelation;
    unsafe { (*CURRENT_RELATIONS).remove(&*relation); }
}

#[no_mangle]
pub extern "C" fn print_relation_name(relation_c_ptr: *const c_void) {
    let relation = relation_c_ptr as *const ImmediateRelation;
    println!("Relation name: {}", unsafe { (*relation).get_name() });
}

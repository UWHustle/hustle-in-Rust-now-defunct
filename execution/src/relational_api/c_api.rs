use relational_api::rust_api::ImmediateRelation;
use std::ffi::*;
use std::os::raw::c_char;
use storage::storage_manager::Value;

#[no_mangle]
pub unsafe extern "C" fn ffi_get_err_p() -> *const c_void {
    Box::into_raw(Box::new(String::from("No errors"))) as *const c_void
}

#[no_mangle]
pub unsafe extern "C" fn ffi_get_err_str_p(err_p: *const String) -> *const c_void {
    encode_c_str(&*err_p)
}

#[no_mangle]
pub unsafe extern "C" fn ffi_new_relation(
    col_names_p: *const *const c_char,
    type_names_p: *const *const c_char,
    n_cols: u32,
    err_p: *mut String,
) -> *const c_void {
    let col_names = decode_c_str_list(col_names_p, n_cols);
    let type_names = decode_c_str_list(type_names_p, n_cols);
    process_result_p(ImmediateRelation::new(col_names, type_names), err_p)
}

#[no_mangle]
pub unsafe extern "C" fn ffi_get_name_p(relation_p: *const ImmediateRelation) -> *const c_void {
    encode_c_str((*relation_p).get_name()) as *const c_void
}

#[no_mangle]
pub unsafe extern "C" fn ffi_get_col_names_p(
    relation_p: *const ImmediateRelation,
) -> *const c_void {
    encode_c_str_vec((*relation_p).get_col_names()) as *const c_void
}

#[no_mangle]
pub unsafe extern "C" fn ffi_get_type_names_p(
    relation_p: *const ImmediateRelation,
) -> *const c_void {
    encode_c_str_vec((*relation_p).get_col_type_names()) as *const c_void
}

#[no_mangle]
pub unsafe extern "C" fn ffi_get_n_cols(relation_p: *const ImmediateRelation) -> u32 {
    (*relation_p).get_col_names().len() as u32
}

#[no_mangle]
pub unsafe extern "C" fn ffi_get_data_p(relation_p: *const ImmediateRelation) -> *const c_void {
    match (*relation_p).get_data() {
        Some(value) => Box::into_raw(Box::new(value)) as *const c_void,
        None => 0 as *const c_void,
    }
}

#[no_mangle]
pub unsafe extern "C" fn ffi_get_slice_p(data_p: *const Value) -> *const c_void {
    let slice: &[u8] = &**data_p;
    let slice_p = slice.as_ptr() as *const c_void;
    slice_p
}

#[no_mangle]
pub unsafe extern "C" fn ffi_get_slice_size(data_p: *const Value) -> u32 {
    let slice: &[u8] = &**data_p;
    slice.len() as u32
}

#[no_mangle]
pub unsafe extern "C" fn ffi_get_str_i(vec_p: *mut Vec<*const c_void>, i: usize) -> *const c_void {
    (*vec_p)[i] as *const c_void
}

#[no_mangle]
pub unsafe extern "C" fn ffi_drop_err_p(err_p: *mut String) {
    Box::from_raw(err_p);
}

#[no_mangle]
pub unsafe extern "C" fn ffi_drop_relation(relation_p: *mut ImmediateRelation) {
    Box::from_raw(relation_p);
}

#[no_mangle]
pub unsafe extern "C" fn ffi_drop_c_str(c_str_p: *mut c_char) {
    CString::from_raw(c_str_p);
}

#[no_mangle]
pub unsafe extern "C" fn ffi_drop_c_str_vec(vec_p: *mut Vec<*mut c_char>) {
    let c_str_list = Box::from_raw(vec_p);
    for c_str in *c_str_list {
        ffi_drop_c_str(c_str);
    }
}

#[no_mangle]
pub unsafe extern "C" fn ffi_drop_data(data_p: *mut Value) {
    Box::from_raw(data_p);
}

#[no_mangle]
pub unsafe extern "C" fn ffi_copy_buffer(
    relation_p: *const ImmediateRelation,
    buffer: *const u8,
    size: usize,
) {
    let slice = std::slice::from_raw_parts(buffer, size);
    (*relation_p).copy_slice(slice);
}

#[no_mangle]
pub unsafe extern "C" fn ffi_import_hustle(
    relation_p: *const ImmediateRelation,
    name_p: *const c_char,
    err_p: *mut String,
) -> i32 {
    process_result_i((*relation_p).import_hustle(decode_c_str(name_p)), err_p)
}

#[no_mangle]
pub unsafe extern "C" fn ffi_export_hustle(
    relation_p: *const ImmediateRelation,
    name_p: *const c_char,
    err_p: *mut String,
) -> i32 {
    process_result_i((*relation_p).export_hustle(decode_c_str(name_p)), err_p)
}

#[no_mangle]
pub unsafe extern "C" fn ffi_import_csv(
    relation_p: *const ImmediateRelation,
    filename_p: *const c_char,
    err_p: *mut String,
) -> i32 {
    process_result_i((*relation_p).import_csv(decode_c_str(filename_p)), err_p)
}

#[no_mangle]
pub unsafe extern "C" fn ffi_export_csv(
    relation_p: *const ImmediateRelation,
    filename_p: *const c_char,
    err_p: *mut String,
) -> i32 {
    process_result_i((*relation_p).export_csv(decode_c_str(filename_p)), err_p)
}

#[no_mangle]
pub unsafe extern "C" fn ffi_aggregate(
    relation_p: *const ImmediateRelation,
    agg_col_name_p: *const c_char,
    group_by_col_names_p: *const *const c_char,
    n_group_by: u32,
    agg_func_p: *const c_char,
    err_p: *mut String,
) -> *const c_void {
    let agg_col_name = decode_c_str(agg_col_name_p);
    let group_by_col_names = decode_c_str_list(group_by_col_names_p, n_group_by);
    let agg_name = decode_c_str(agg_func_p);
    process_result_p(
        (*relation_p).aggregate(agg_col_name, group_by_col_names, agg_name),
        err_p,
    )
}

#[no_mangle]
pub unsafe extern "C" fn ffi_insert(
    relation_p: *const ImmediateRelation,
    value_strings_p: *const *const c_char,
    n_values: u32,
    err_p: *mut String,
) -> i32 {
    process_result_i(
        (*relation_p).insert(decode_c_str_list(value_strings_p, n_values)),
        err_p,
    )
}

#[no_mangle]
pub unsafe extern "C" fn ffi_join(
    relation1_p: *const ImmediateRelation,
    relation2_p: *const ImmediateRelation,
    err_p: *mut String,
) -> *const c_void {
    process_result_p((*relation1_p).join(&*relation2_p), err_p)
}

#[no_mangle]
pub unsafe extern "C" fn ffi_limit(
    relation_p: *const ImmediateRelation,
    limit: u32,
    err_p: *mut String,
) -> *const c_void {
    process_result_p((*relation_p).limit(limit), err_p)
}

#[no_mangle]
pub unsafe extern "C" fn ffi_print(
    relation_p: *const ImmediateRelation,
    err_p: *mut String,
) -> i32 {
    process_result_i((*relation_p).print(), err_p)
}

#[no_mangle]
pub unsafe extern "C" fn ffi_project(
    relation_p: *const ImmediateRelation,
    col_names_p: *const *const c_char,
    n_cols: u32,
    err_p: *mut String,
) -> *const c_void {
    let col_names = decode_c_str_list(col_names_p, n_cols);
    process_result_p((*relation_p).project(col_names), err_p)
}

#[no_mangle]
pub unsafe extern "C" fn ffi_select(
    relation_p: *const ImmediateRelation,
    predicate_p: *const c_char,
    err_p: *mut String,
) -> *const c_void {
    let predicate = decode_c_str(predicate_p);
    process_result_p((*relation_p).select(predicate), err_p)
}

unsafe fn encode_c_str(string: &str) -> *const c_void {
    CString::new(string).unwrap().into_raw() as *const c_void
}

unsafe fn encode_c_str_vec(list: Vec<String>) -> *const c_void {
    let mut encoded: Vec<*const c_void> = vec![];
    for string in list {
        encoded.push(encode_c_str(&string));
    }
    Box::into_raw(Box::new(encoded)) as *const c_void
}

unsafe fn decode_c_str<'a>(c_str: *const c_char) -> &'a str {
    CStr::from_ptr(c_str).to_str().expect("Invalid utf8-string")
}

unsafe fn decode_c_str_list<'a>(c_str_list: *const *const c_char, length: u32) -> Vec<&'a str> {
    let length = length as isize;
    let mut decoded: Vec<&str> = vec![];
    for i in 0..length {
        let c_str = *c_str_list.offset(i);
        decoded.push(decode_c_str(c_str));
    }
    decoded
}

unsafe fn process_result_i<T>(result: Result<T, String>, err_p: *mut String) -> i32 {
    match result {
        Ok(_val) => 0,
        Err(string) => {
            (*err_p).truncate(0);
            (*err_p).push_str(&string);
            -1
        }
    }
}

unsafe fn process_result_p<T, U>(result: Result<T, String>, err_p: *mut String) -> *const U {
    match result {
        Ok(val) => Box::into_raw(Box::new(val)) as *const U,
        Err(string) => {
            (*err_p).truncate(0);
            (*err_p).push_str(&string);
            0 as *const U
        }
    }
}

use relational_api::rust_api::ImmediateRelation;
use std::ffi::*;
use std::os::raw::c_char;

#[no_mangle]
pub unsafe extern "C" fn ffi_new_relation(
    col_names_ptr: *const *const c_char,
    col_type_names_ptr: *const *const c_char,
    n_cols: u32) -> *const c_void
{
    let col_names = decode_c_str_list(col_names_ptr, n_cols);
    let col_type_names = decode_c_str_list(col_type_names_ptr, n_cols);
    let relation = Box::new(ImmediateRelation::new(col_names, col_type_names));
    Box::into_raw(relation) as *const c_void
}

#[no_mangle]
pub unsafe extern "C" fn ffi_drop_relation(relation_ptr: *mut c_void) {
    Box::from_raw(relation_ptr as *mut ImmediateRelation);
}

#[no_mangle]
pub unsafe extern "C" fn ffi_get_name(relation_ptr: *const c_void) -> *const c_void {
    let relation = relation_ptr as *const ImmediateRelation;
    CString::new((*relation).get_name())
        .expect("Unable to construct C string")
        .into_raw() as *const c_void
}

#[no_mangle]
pub unsafe extern "C" fn ffi_drop_c_str(c_str: *mut c_void) {
    CString::from_raw(c_str as *mut c_char);
}

#[no_mangle]
pub unsafe extern "C" fn ffi_import_hustle(relation_ptr: *const c_void, name_ptr: *const c_char) {
    let relation = relation_ptr as *const ImmediateRelation;
    let name = decode_c_str(name_ptr);
    (*relation).import_hustle(name);
}

#[no_mangle]
pub unsafe extern "C" fn ffi_export_hustle(relation_ptr: *const c_void, name_ptr: *const c_char) {
    let relation = relation_ptr as *const ImmediateRelation;
    let name = decode_c_str(name_ptr);
    (*relation).export_hustle(name);
}

#[no_mangle]
pub unsafe extern "C" fn ffi_import_csv(relation_ptr: *const c_void, filename_ptr: *const c_char) {
    let relation = relation_ptr as *const ImmediateRelation;
    let filename = decode_c_str(filename_ptr);
    (*relation).import_csv(filename);
}

#[no_mangle]
pub unsafe extern "C" fn ffi_export_csv(relation_ptr: *const c_void, filename_ptr: *const c_char) {
    let relation = relation_ptr as *const ImmediateRelation;
    let filename = decode_c_str(filename_ptr);
    (*relation).export_csv(filename);
}

#[no_mangle]
pub unsafe extern "C" fn ffi_aggregate(
    relation_ptr: *const c_void,
    agg_col_name_ptr: *const c_char,
    group_by_col_names_ptr: *const *const c_char,
    n_group_by_cols: u32,
    agg_name_ptr: *const c_char) -> *const c_void
{
    let relation = relation_ptr as *const ImmediateRelation;
    let agg_col_name = decode_c_str(agg_col_name_ptr);
    let group_by_col_names = decode_c_str_list(group_by_col_names_ptr, n_group_by_cols);
    let agg_name = decode_c_str(agg_name_ptr);
    let output = (*relation).aggregate(agg_col_name, group_by_col_names, agg_name);
    Box::into_raw(Box::new(output)) as *const c_void
}

#[no_mangle]
pub unsafe extern "C" fn ffi_join(
    relation1_ptr: *const c_void,
    relation2_ptr: *const c_void) -> *const c_void
{
    let relation1 = relation1_ptr as *const ImmediateRelation;
    let relation2 = relation2_ptr as *const ImmediateRelation;
    let output = (*relation1).join(&*relation2);
    Box::into_raw(Box::new(output)) as *const c_void
}

#[no_mangle]
pub unsafe extern "C" fn ffi_limit(relation_ptr: *const c_void, limit: u32) -> *const c_void {
    let relation = relation_ptr as *const ImmediateRelation;
    let output = (*relation).limit(limit);
    Box::into_raw(Box::new(output)) as *const c_void
}

#[no_mangle]
pub unsafe extern "C" fn ffi_print(relation_ptr: *const c_void) {
    let relation = relation_ptr as *const ImmediateRelation;
    (*relation).print();
}

#[no_mangle]
pub unsafe extern "C" fn ffi_project(
    relation_ptr: *const c_void,
    col_names_ptr: *const *const c_char,
    n_cols: u32) -> *const c_void
{
    let relation = relation_ptr as *const ImmediateRelation;
    let col_names = decode_c_str_list(col_names_ptr, n_cols);
    let output = (*relation).project(col_names);
    Box::into_raw(Box::new(output)) as *const c_void
}

#[no_mangle]
pub unsafe extern "C" fn ffi_select(
    relation_ptr: *const c_void,
    predicate_ptr: *const c_char) -> *const c_void
{
    let relation = relation_ptr as *const ImmediateRelation;
    let predicate = decode_c_str(predicate_ptr);
    let output = (*relation).select(predicate);
    Box::into_raw(Box::new(output)) as *const c_void
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

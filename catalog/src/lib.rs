extern crate serde;
extern crate serde_json;

#[macro_use]
extern crate serde_derive;

pub mod entities;
pub mod helpers;

use helpers::api;
use entities::database::Database;

use std::os::raw::c_char;
use helpers::string_helper::from_cstr;
use helpers::string_helper::to_cstr;

use std::mem::forget;


/*
High level API design:

Schema Modifications:
    A user requests a snapshot of a specific database's catalog.
        This returns a token referencing that snapshot.
    That user can then call API functions to read or modify the schema sections of the catalog.
    That user can then commit their snapshot to the catalog.

    Upon committing the snapshot all changes are reflected in the catalog atomically.
        If a conflicting change occurred while the lock was held, an error is returned.

Statistic Updates:
    Statistics are expected to change frequently compared to the schema.
        Changes to statistics are committed directly but may not reflect immediately.
        Only certain actions are allowed on statistics so that the order of application doesn't matter.

*/


// Obtains and locks the database level catalog
#[no_mangle]
pub extern fn lock_db(name: *const c_char) -> *const c_char {
    forget(name);
    to_cstr(api::lock_db(from_cstr(name)))
}

// Saves and unlocks the database level catalog
#[no_mangle]
pub extern fn release_db(serialized_db: *const c_char) -> *const c_char {
    std::mem::forget(serialized_db);
    api::release_db(from_cstr(serialized_db));
    to_cstr("ok".to_string())
}



// Get a database's name
#[no_mangle]
pub extern fn get_db_name(serialized_db: *const c_char) -> *const c_char {
    std::mem::forget(serialized_db);
    to_cstr(api::get_db_name(from_cstr(serialized_db)))
}



// Get a database's relations names
#[no_mangle]
pub extern fn get_relations(serialized_db: *const c_char) -> *const *const c_char {
    forget(serialized_db);
    let db = Database::deserialize(from_cstr(serialized_db)).unwrap();
    let result:Vec<_> = db.relations.into_iter().map(|rel| to_cstr(rel.name)).collect();
    let result_ptr = result.as_ptr();
    forget(result);
    result_ptr
}

// Get a database's relation count
#[no_mangle]
pub extern fn get_relation_count(serialized_db: *const c_char) -> u8 {
    forget(serialized_db);
    let db = Database::deserialize(from_cstr(serialized_db)).unwrap();
    db.relations.len() as u8
}

#[no_mangle]
pub extern fn add_relation(serialized_db: *const c_char, relation_name: *const c_char) -> *const c_char {
    forget(serialized_db);
    forget(relation_name);
    let s_db = from_cstr(serialized_db);
    let rel_name = from_cstr(relation_name);

    let new_serialized_db = api::add_relation(s_db,rel_name);

    let result = to_cstr(new_serialized_db);
    std::mem::forget(serialized_db);
    std::mem::forget(relation_name);
    std::mem::forget(result);
    result
}



// Get a database's relation count
#[no_mangle]
pub extern fn get_relation_column_count(serialized_db: *const c_char, relation_name: *const c_char) -> u8 {
    forget(serialized_db);
    forget(relation_name);
    let result = api::get_relation_column_names(from_cstr(serialized_db),
                                           from_cstr(relation_name));
    result.len() as u8
}

// Get a specific relation's columns names
#[no_mangle]
pub extern fn get_relation_columns(serialized_db: *const c_char, relation_name: *const c_char) ->  *const *const c_char {
    forget(serialized_db);
    forget(relation_name);
    let result = api::get_relation_column_names(from_cstr(serialized_db),
                                       from_cstr(relation_name));
    let external_result:Vec<_> = result.into_iter().map(|col_name| to_cstr(col_name)).collect();
    let result_ptr = external_result.as_ptr();
    forget(external_result);
    result_ptr
}

#[no_mangle]
pub extern fn add_column(serialized_db: *const c_char, relation_name: *const c_char, column_name: *const c_char) -> *const c_char {
    forget(serialized_db);
    forget(relation_name);
    forget(column_name);

    let new_serialized_db = api::add_column(from_cstr(serialized_db),
                                            from_cstr(relation_name),
                                            from_cstr(column_name));

    let result = to_cstr(new_serialized_db);
    std::mem::forget(result);
    result
}


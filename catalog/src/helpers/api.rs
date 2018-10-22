extern crate serde;
extern crate serde_json;

use entities::database::Database;

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
pub extern fn lock_db(name: String) -> String {
    Database::load(name).unwrap().serialize().unwrap()
}

// Saves and unlocks the database level catalog
pub extern fn release_db(serialized_db: String) -> () {
    let mut db = Database::deserialize(serialized_db).unwrap();
    db.save().unwrap();
}



// Get a database's name
pub extern fn get_db_name(serialized_db: String) -> String {
    Database::deserialize(serialized_db.clone()).unwrap().name
}

pub extern fn delete_db(serialized_db: String) -> () {
    Database::delete(&Database::deserialize(serialized_db.clone()).unwrap()).unwrap();
}



// Get a database's relations names
pub extern fn get_relations(serialized_db: String) -> Vec<String> {
    let db = Database::deserialize(serialized_db).unwrap();
    db.relations.into_iter().map(|rel| rel.name).collect()
}

pub extern fn add_relation(serialized_db: String, relation_name: String) -> String {
    let mut db = Database::deserialize(serialized_db).unwrap();
    db.add_relation(relation_name);
    db.serialize().unwrap()
}

// Get a specific relation's columns names
pub extern fn get_relation_column_names(serialized_db: String, relation_name: String) -> Vec<String> {
    let mut db = Database::deserialize(serialized_db).unwrap();
    let relation = db.get_relation(relation_name);
    let result:Vec<_> = relation.get_columns().into_iter().map(|col| col.name).collect();
    result
}

pub extern fn add_column(serialized_db: String, relation_name: String, column_name: String) -> String {
    let mut db = Database::deserialize(serialized_db).unwrap();
    {
        let relation = db.get_relation(relation_name);
        relation.add_column(column_name);
    }
    db.serialize().unwrap()
}

// Get a specific column's data type
pub extern fn get_column_type(serialized_db: String, relation_name: String, column_name: String) -> String {
    let db = Database::deserialize(serialized_db).unwrap();
    let relation = db.relations.into_iter().find(|rel| rel.name == relation_name).unwrap();
    let column = relation.columns.into_iter().find(|col| col.name == column_name).unwrap();
    column.datatype.name
}


#[cfg(test)]
mod tests {
    use helpers::api::lock_db;
    use helpers::api::release_db;
    use helpers::api::delete_db;

    #[test]
    fn db_name() {
        use helpers::api::get_db_name;

        let db_name =  "test_db1".to_string();

        let db = lock_db(db_name.clone());
        assert_eq!(get_db_name(db.clone()), db_name.clone());
        release_db(db.clone());
        delete_db(db.clone());
    }

    #[test]
    fn relations() {
        use helpers::api::get_relations;
        use helpers::api::add_relation;

        let db_name =  "test_db2".to_string();

        let db = lock_db(db_name.clone());
        let existing_relations = get_relations(db.clone());
        let db = add_relation(db.clone(), "My_new_relation".to_string());
        let new_relations = get_relations(db.clone());
        assert_eq!(existing_relations.len()+1, new_relations.len());
        release_db(db.clone());

        let db2 = lock_db(db_name.clone());
        let saved_relations = get_relations(db2.clone());
        assert_eq!(new_relations.len(), saved_relations.len());
        release_db(db2.clone());
        delete_db(db2.clone());
    }

    #[test]
    fn columns() {
        use helpers::api::add_relation;
        use helpers::api::get_relations;
        use helpers::api::add_column;
        use helpers::api::get_relation_column_names;

        let db_name =  "test_db3".to_string();

        let db = lock_db(db_name.clone());
        let db = add_relation(db.clone(), "My_new_relation".to_string());
        let existing_relations = get_relations(db.clone());
        let relation = existing_relations.first().unwrap().clone();

        let existing_columns = get_relation_column_names(db.clone(), relation.clone());
        let db = add_column(db.clone(), relation.clone(), "my_new_column".to_string());
        let new_columns = get_relation_column_names(db.clone(), relation.clone());
        assert_eq!(existing_columns.len()+1, new_columns.len());
        release_db(db.clone());

        let db2 = lock_db(db_name.clone());
        let saved_columns = get_relation_column_names(db.clone(), relation.clone());
        assert_eq!(new_columns.len(), saved_columns.len());
        release_db(db2.clone());
        delete_db(db2.clone());
    }

}
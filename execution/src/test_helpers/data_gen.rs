use test_helpers::sqlite3::import_csv_to_sqlite3;

use logical_entities::column::Column;
use logical_entities::relation::Relation;
use logical_entities::row::Row;
use logical_entities::schema::Schema;
use physical_operators::export_csv::ExportCsv;
use physical_operators::import_csv::ImportCsv;
use physical_operators::insert::Insert;
use physical_operators::test_relation::TestRelation;
use physical_operators::Operator;
use type_system::type_id::*;
use type_system::*;

extern crate storage;
use self::storage::StorageManager;

pub fn generate_relation_t_into_hustle_and_sqlite3(record_count: usize, random: bool) -> Relation {
    let relation = Relation::new(
        "T",
        Schema::new(vec![
            Column::new("a".to_string(), TypeID::new(Variant::Int4, true)),
            Column::new("b".to_string(), TypeID::new(Variant::Int4, true)),
        ]),
    );

    let csv_file = "test-data/data.csv".to_string();

    generate_data(relation.clone(), record_count, random);
    export_csv(csv_file.clone(), relation.clone());

    import_csv_to_sqlite3();
    import_csv_to_hustle(csv_file.clone(), relation.clone());
    relation
}

pub fn generate_relation_a_into_hustle_and_sqlite3(record_count: usize) -> Relation {
    let relation = Relation::new(
        "A",
        Schema::new(vec![
            Column::new("w".to_string(), TypeID::new(Variant::Int4, true)),
            Column::new("x".to_string(), TypeID::new(Variant::Int4, true)),
            Column::new("y".to_string(), TypeID::new(Variant::Int4, true)),
            Column::new("z".to_string(), TypeID::new(Variant::Int4, true)),
        ]),
    );

    let csv_file = "test-data/data.csv".to_string();

    generate_data(relation.clone(), record_count, false);
    export_csv(csv_file.clone(), relation.clone());
    import_csv_to_sqlite3();
    relation
}

pub fn generate_relation_b_into_hustle_and_sqlite3(record_count: usize) -> Relation {
    let relation = Relation::new(
        "B",
        Schema::new(vec![
            Column::new("w".to_string(), TypeID::new(Variant::Int4, true)),
            Column::new("x".to_string(), TypeID::new(Variant::Int4, true)),
        ]),
    );

    let csv_file = "test-data/data.csv".to_string();

    generate_data(relation.clone(), record_count, false);
    export_csv(csv_file.clone(), relation.clone());

    import_csv_to_sqlite3();
    import_csv_to_hustle(csv_file.clone(), relation.clone());
    relation
}

pub fn generate_data(relation: Relation, record_count: usize, random: bool) {
    let test_relation_generator = TestRelation::new(relation.clone(), record_count, random);
    test_relation_generator.execute(&StorageManager::new());
}

pub fn export_csv(csv_file: String, relation: Relation) {
    use std::process::Command;
    Command::new("rm").arg(csv_file.clone()).output().unwrap();

    let export_csv = ExportCsv::new(csv_file.clone(), relation.clone());
    export_csv.execute(&StorageManager::new());
}

pub fn import_csv_to_hustle(csv_file: String, relation: Relation) {
    let import_operator = ImportCsv::new(csv_file.clone(), relation.clone());
    import_operator.execute(&StorageManager::new());
}

pub fn insert_into_hustle(count: u8, value: &Value, relation: Relation) {
    let insert_operator = Insert::new(
        relation.clone(),
        Row::new(
            relation.get_schema().clone(),
            vec![value.box_clone_value(), value.box_clone_value()],
        ),
    );
    for _ in 0..count {
        insert_operator.execute(&StorageManager::new());
    }
}

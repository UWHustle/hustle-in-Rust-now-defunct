use test_helpers::sqlite3::import_csv_to_sqlite3;

use logical_entities::types::ValueType;
use logical_entities::relation::Relation;
use logical_entities::column::Column;
use logical_entities::schema::Schema;
use logical_entities::row::Row;

use physical_operators::import_csv::ImportCsv;
use physical_operators::insert::Insert;
use physical_operators::random_relation::RandomRelation;
use physical_operators::export_csv::ExportCsv;
use logical_entities::types::*;

use physical_operators::Operator;


pub fn generate_relation_into_hustle_and_sqlite3(record_count: usize) -> Relation {
    let relation = Relation::new("T".to_string(),
                                 Schema::new(vec!(Column::new("a".to_string(), TypeID::Int4(true)),
                                                  Column::new("b".to_string(), TypeID::Int4(true))
                                 )));

    let csv_file = "test-data/data.csv".to_string();

    generate_data(relation.clone(), record_count);
    export_csv(csv_file.clone(), relation.clone());

    import_csv_to_sqlite3();
    import_csv_to_hustle(csv_file.clone(), relation.clone());
    relation
}

pub fn generate_relation_a_into_hustle_and_sqlite3(record_count: usize) -> Relation {
    let relation = Relation::new("A".to_string(),
                                 Schema::new(vec!(Column::new("w".to_string(), TypeID::Int4(true)),
                                                  Column::new("x".to_string(), TypeID::Int4(true)),
                                                  Column::new("y".to_string(), TypeID::Int4(true)),
                                                  Column::new("z".to_string(), TypeID::Int4(true))
                                 )));

    let csv_file = "test-data/data.csv".to_string();

    generate_data(relation.clone(), record_count);
    export_csv(csv_file.clone(), relation.clone());

    import_csv_to_sqlite3();
    import_csv_to_hustle(csv_file.clone(), relation.clone());
    relation
}

pub fn generate_relation_b_into_hustle_and_sqlite3(record_count: usize) -> Relation {
    let relation = Relation::new("B".to_string(),
                                 Schema::new(vec!(Column::new("w".to_string(), TypeID::Int4(true)),
                                                  Column::new("x".to_string(), TypeID::Int4(true))
                                 )));

    let csv_file = "test-data/data.csv".to_string();

    generate_data(relation.clone(), record_count);
    export_csv(csv_file.clone(), relation.clone());

    import_csv_to_sqlite3();
    import_csv_to_hustle(csv_file.clone(), relation.clone());
    relation
}

pub fn generate_data(relation: Relation, record_count: usize) {
    let random_relation_generator = RandomRelation::new(relation.clone(), record_count);
    random_relation_generator.execute();
}

pub fn export_csv(csv_file: String, relation: Relation) {
    use std::process::Command;
    Command::new("rm").arg(csv_file.clone()).output().unwrap();

    let export_csv = ExportCsv::new(csv_file.clone(), relation.clone());
    export_csv.execute();
}

pub fn import_csv_to_hustle(csv_file: String, relation: Relation) {
    let import_operator = ImportCsv::new(csv_file.clone(), relation.clone());
    import_operator.execute();
}

pub fn insert_into_hustle(count: u8, value: &ValueType, relation: Relation) {
    let insert_operator = Insert::new(relation.clone(), Row::new(relation.get_schema().clone(), vec!(value.box_clone(), value.box_clone())));
    for _ in 0..count {
        insert_operator.execute();
    }
}

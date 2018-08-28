extern crate hustle;

use hustle::logical_entities::relation::Relation;
use hustle::logical_entities::column::Column;
use hustle::logical_entities::schema::Schema;
use hustle::logical_entities::row::Row;

use hustle::physical_operators::generate_data::GenerateData;
use hustle::physical_operators::import_csv::ImportCsv;
use hustle::physical_operators::insert::Insert;
use hustle::physical_operators::join::Join;
use hustle::physical_operators::select_sum::SelectSum;
use hustle::physical_operators::select_output::SelectOutput;

extern crate csv;

use std::process::Command;

use std::time::{Instant};

const GENERATE_DATA: bool = true; // Generates the CSV File with Data
const RECORD_COUNT: usize = 1024;

const CONVERT_DATA_TO_SQLITE: bool = true; // Loads data from CSV file into sqlite3
const CONVERT_DATA_TO_HUSTLE: bool = true; // Loads data from CSV file into hustle





#[test]
fn test_flow() {
    let relation = Relation::new("T".to_string(),
                                            Schema::new(vec!(Column::new("a".to_string(),8),
                                                             Column::new("b".to_string(),8)
                                            )));

    let csv_file = "test-data/data.csv".to_string();

    generate_csv_data(csv_file.clone(), relation.clone());
    import_csv_to_sqlite3();
    import_csv_to_hustle(csv_file.clone(), relation.clone());


    let hustle_calculation = sum_column_hustle(relation.clone(), "b".to_string());
    let sqlite3_calculation = run_query_sqlite3("SELECT SUM(b) FROM T;".to_string());
    assert_eq!(hustle_calculation, sqlite3_calculation);


    let join_relation = hustle_join(relation.clone(), relation.clone());
    let hustle_calculation = sum_column_hustle(join_relation.clone(), "b".to_string());
    let sqlite3_calculation = run_query_sqlite3("SELECT SUM(t1.b)+SUM(t2.b) FROM t as t1 JOIN t as t2;".to_string());
    assert_eq!(hustle_calculation, sqlite3_calculation);


    insert_into_hustle(10, 3,  relation.clone());
    let hustle_calculation = sum_column_hustle(relation.clone(), "b".to_string());
    let sqlite3_calculation = run_query_sqlite3("SELECT SUM(b) FROM T;".to_string());
    assert_eq!(hustle_calculation, sqlite3_calculation+30);

}




fn generate_csv_data(csv_file: String, relation: Relation){

    Command::new("rm").arg(csv_file.clone()).output().unwrap();

    let data_generator = GenerateData::new(csv_file.clone(), relation.clone(), RECORD_COUNT);
    data_generator.execute();
}

fn import_csv_to_sqlite3(){
    Command::new("bash").arg("scripts/sqlite_import.sh").output().unwrap();
}

fn import_csv_to_hustle(csv_file: String, relation:Relation){
    let import_operator = ImportCsv::new(csv_file.clone(), relation.clone());
    import_operator.execute();
}

fn insert_into_hustle(count: u8, value: u64, relation: Relation){
    let insert_operator = Insert::new(relation.clone(), Row::new(relation.get_schema().clone(),vec!(value,value)));
    for i in 0..count {
        insert_operator.execute();
    }
}

fn sum_column_hustle(relation: Relation, column_name: String) -> u128 {
    let select_operator = SelectSum::new(relation.clone(),Column::new(column_name, 8));
    select_operator.execute()
}

fn hustle_join(relation1:Relation, relation2:Relation) -> Relation {
    let join_operator = Join::new(relation1.clone(), relation2.clone());
    join_operator.execute()
}

fn run_query_sqlite3(query : String) -> u128{
    extern crate sqlite;
    let now = Instant::now();
    let connection = sqlite::open("test-data/sqlite.data").unwrap();
    let mut output_value:u128 = 0;
    connection
        .iterate(query , |pairs| {
            for &(column, value) in pairs.iter() {
                output_value = value.unwrap().to_string().parse::<u128>().unwrap();
            }
            true
        })
        .expect("Query failed.");
    output_value
}
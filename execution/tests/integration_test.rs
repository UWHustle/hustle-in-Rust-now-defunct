extern crate execution;

use execution::logical_entities::relation::Relation;
use execution::logical_entities::column::Column;
use execution::logical_entities::schema::Schema;
use execution::logical_entities::row::Row;

use execution::physical_operators::import_csv::ImportCsv;
use execution::physical_operators::insert::Insert;
use execution::physical_operators::join::Join;
use execution::physical_operators::select_sum::SelectSum;
use execution::physical_operators::random_relation::RandomRelation;
use execution::physical_operators::export_csv::ExportCsv;

use execution::logical_entities::value::Value;
use execution::logical_entities::types::DataType;

use execution::physical_operators::Operator;

extern crate csv;

use std::process::Command;

const RECORD_COUNT: usize = 512;

#[test]
fn test_flow() {
    let relation = Relation::new("T".to_string(),
                                            Schema::new(vec!(Column::new("a".to_string(),8),
                                                             Column::new("b".to_string(),8)
                                            )));

    let csv_file = "test-data/data.csv".to_string();

    generate_data(relation.clone());
    export_csv(csv_file.clone(), relation.clone());

    import_csv_to_sqlite3();
    import_csv_to_hustle(csv_file.clone(), relation.clone());


    let hustle_calculation = sum_column_hustle(relation.clone(), "b".to_string());
    let sqlite3_calculation = run_query_sqlite3("SELECT SUM(b) FROM T;".to_string());
    assert_eq!(hustle_calculation, sqlite3_calculation);


    let join_relation = hustle_join(relation.clone(), relation.clone());
    let hustle_calculation = sum_column_hustle(join_relation.clone(), "b".to_string());
    let sqlite3_calculation = run_query_sqlite3("SELECT SUM(t1.b)+SUM(t2.b) FROM t as t1 JOIN t as t2;".to_string());
    assert_eq!(hustle_calculation, sqlite3_calculation);


    let insert_value =  DataType::Integer.parse_to_value("3".to_string());
    insert_into_hustle(10, insert_value,  relation.clone());
    let hustle_calculation = sum_column_hustle(relation.clone(), "b".to_string());
    let sqlite3_calculation = run_query_sqlite3("SELECT SUM(b) FROM T;".to_string());
    assert_eq!(hustle_calculation, sqlite3_calculation+30);

    /*
    use execution::physical_operators::select_output::Scan;
    let mut select_operator = Scan::new(relation.clone());
    select_operator.execute();
    select_operator.print();
    */
}


fn generate_data(relation: Relation){
    let random_relation_generator = RandomRelation::new(relation.clone(), RECORD_COUNT);
    random_relation_generator.execute();
}

fn export_csv(csv_file: String, relation: Relation) {
    Command::new("rm").arg(csv_file.clone()).output().unwrap();

    let export_csv = ExportCsv::new(csv_file.clone(), relation.clone(), RECORD_COUNT);
    export_csv.execute();
}

fn import_csv_to_sqlite3(){
    Command::new("bash").arg("scripts/sqlite_import.sh").output().unwrap();
}

fn import_csv_to_hustle(csv_file: String, relation:Relation){
    let import_operator = ImportCsv::new(csv_file.clone(), relation.clone());
    import_operator.execute();
}

fn insert_into_hustle(count: u8, value: Value, relation: Relation){
    let insert_operator = Insert::new(relation.clone(), Row::new(relation.get_schema().clone(),vec!(value.clone(),value.clone())));
    for _ in 0..count {
        insert_operator.execute();
    }
}

fn sum_column_hustle(relation: Relation, column_name: String) -> u128 {
    let select_operator = SelectSum::new(relation.clone(),Column::new(column_name, 8));
    select_operator.execute().parse::<u128>().unwrap()
}

fn hustle_join(relation1:Relation, relation2:Relation) -> Relation {
    let join_operator = Join::new(relation1.clone(), relation2.clone());
    join_operator.execute()
}

fn run_query_sqlite3(query : String) -> u128{
    extern crate sqlite;
    let connection = sqlite::open("test-data/sqlite.data").unwrap();
    let mut output_value:u128 = 0;
    connection
        .iterate(query , |pairs| {
            for &(_column, value) in pairs.iter() {
                output_value = value.unwrap().to_string().parse::<u128>().unwrap();
            }
            true
        })
        .expect("Query failed.");
    output_value
}
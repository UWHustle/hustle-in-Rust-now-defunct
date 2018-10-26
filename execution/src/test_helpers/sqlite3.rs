pub fn run_query_sqlite3(query : String) -> u128{
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

pub fn import_csv_to_sqlite3(){
    use std::process::Command;
    Command::new("bash").arg("scripts/sqlite_import.sh").output().unwrap();
}
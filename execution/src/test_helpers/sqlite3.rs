pub fn run_query_sqlite3(query: &str, column_name: &str) -> u128 {
    extern crate sqlite;
    let connection = sqlite::open("test-data/sqlite.data").unwrap();
    let mut output_value: u128 = 0;
    connection
        .iterate(query.to_string(), |pairs| {
            for &(_column, value) in pairs.iter() {
                if _column == column_name {
                    output_value += value.unwrap().to_string().parse::<u128>().unwrap();
                }
            }
            true
        })
        .expect("Query failed.");
    output_value
}

pub fn import_csv_to_sqlite3() {
    use std::process::Command;
    Command::new("rm")
        .arg("test-data/sqlite.data".to_string())
        .output()
        .unwrap();
    Command::new("bash")
        .arg("scripts/sqlite_import.sh")
        .output()
        .unwrap();
}

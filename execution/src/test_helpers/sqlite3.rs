extern crate sqlite;

pub const FILENAME: &str = "sqlite.data";

pub fn run_query_sqlite(query: &str, column_name: &str) -> i64 {
    let connection = sqlite::open(FILENAME).unwrap();
    let mut output_value: i64 = 0;
    println!("Attempting query...");
    connection
        .iterate(query.to_string(), |pairs| {
            for &(_column, value) in pairs.iter() {
                if _column == column_name {
                    output_value += value.unwrap().to_string().parse::<i64>().unwrap();
                }
            }
            true
        })
        .expect("query failed");
    println!("Finished query");
    output_value
}

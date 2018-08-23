pub mod logical_entities;
pub mod physical_operators;




use std::time::{Instant};



const GENERATE_DATA: bool = true; // Generates the CSV File with Data
const RECORD_COUNT: usize = 1024;

const CONVERT_DATA_TO_SQLITE: bool = true; // Loads data from CSV file into sqlite3
const CONVERT_DATA_TO_HUSTLE: bool = true; // Loads data from CSV file into hustle



fn main() {
    use logical_entities::relation::Relation;
    use logical_entities::column::Column;
    use logical_entities::schema::Schema;
    let relation = Relation::new("T".to_string(),
                                 Schema::new(vec!(Column::new("a".to_string(),8),
                                                      Column::new("b".to_string(),8)
                                        )));

    let csv_file = "test-data/data.csv".to_string();

    if GENERATE_DATA {
        extern crate csv;
        let now = Instant::now();
        use std::process::Command;
        Command::new("rm").arg(csv_file.clone()).output().unwrap();
        use physical_operators::generate_data::GenerateData;
        let data_generator = GenerateData::new(csv_file.clone(), relation.clone(), RECORD_COUNT);
        data_generator.execute();
        println!("Finished CSV generation in {} seconds.", now.elapsed().as_secs());
    }

    if CONVERT_DATA_TO_SQLITE {
        let now = Instant::now();
        use std::process::Command;
        Command::new("bash").arg("scripts/sqlite_import.sh").output().unwrap();
        println!("Finished CSV to SQLite3 load in {} seconds.", now.elapsed().as_secs());
    }

    if CONVERT_DATA_TO_HUSTLE {
        use physical_operators::import_csv::ImportCsv;
        let import_operator = ImportCsv::new(csv_file.clone(), relation.clone());
        import_operator.execute();

        use physical_operators::insert::Insert;
        use logical_entities::row::Row;
        let insert_operator = Insert::new(relation.clone(), Row::new(relation.get_schema().clone(),vec!(1,11)));
        //insert_operator.execute();
        //insert_operator.execute();
    }


    /* Read */

    {
        use physical_operators::select_sum::SelectSum;
        let select_operator = SelectSum::new(relation.clone(),Column::new("b".to_string(), 8));
        select_operator.execute();
    }

    {
        extern crate sqlite;
        let now = Instant::now();
        let connection = sqlite::open("test-data/sqlite.data").unwrap();
        connection
            .iterate("SELECT SUM(b) FROM t;", |pairs| {
                for &(column, value) in pairs.iter() {
                    println!("{} = {}", column, value.expect("Value not valid."));
                }
                true
            })
            .expect("Query failed.");
        println!("Finished SQL Sum After {} seconds.", now.elapsed().as_secs());
    }

    {
        use physical_operators::select_output::SelectOutput;
        let mut select_operator = SelectOutput::new(relation.clone());
        select_operator.execute();
        //select_operator.print();
    }

    {
        use physical_operators::join::Join;
        use physical_operators::select_sum::SelectSum;
        let join_operator = Join::new(relation.clone(), relation.clone());
        let joined_relation = join_operator.execute();
        let mut select_operator = SelectSum::new(joined_relation.clone(),Column::new("b".to_string(), 8));
        select_operator.execute();

        use physical_operators::select_output::SelectOutput;
        let mut select_operator = SelectOutput::new(joined_relation.clone());
        select_operator.execute();
       //select_operator.print();
    }

    {
        extern crate sqlite;
        let now = Instant::now();
        let connection = sqlite::open("test-data/sqlite.data").unwrap();
        connection
            .iterate("SELECT SUM(t1.b)+SUM(t2.b) FROM t as t1 JOIN t as t2;", |pairs| {
                for &(column, value) in pairs.iter() {
                    println!("{} = {}", column, value.expect("Value not valid."));
                }
                true
            })
            .expect("Query failed.");
        println!("Finished SQL Join After {} seconds.", now.elapsed().as_secs());
    }
}

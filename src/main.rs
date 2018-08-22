pub mod physical_operator;
pub mod logical_operator;




use std::time::{Instant};



const GENERATE_DATA: bool = true; // Generates the CSV File with Data
const RECORD_COUNT: usize = 1024;

const CONVERT_DATA_TO_SQLITE: bool = true; // Loads data from CSV file into sqlite3
const CONVERT_DATA_TO_HUSTLE: bool = true; // Loads data from CSV file into hustle



fn main() {
    use logical_operator::relation::Relation;
    use logical_operator::column::Column;
    let relation = Relation::new("T".to_string(),
                                 vec!(Column::new("a".to_string(),8),
                                                      Column::new("b".to_string(),8)
                                        ));

    if GENERATE_DATA {
        extern crate csv;
        let now = Instant::now();
        use std::process::Command;
        Command::new("rm").arg("data.csv").output().unwrap();
        use physical_operator::data_generator::DataGenerator;
        let data_generator = DataGenerator::new("data.csv".to_string(), relation.clone(), RECORD_COUNT);
        data_generator.execute();
        println!("Finished CSV generation in {} seconds.", now.elapsed().as_secs());
    }

    if CONVERT_DATA_TO_SQLITE {
        let now = Instant::now();
        use std::process::Command;
        Command::new("bash").arg("sqlite_import.sh").output().unwrap();
        println!("Finished CSV to SQLite3 load in {} seconds.", now.elapsed().as_secs());
    }

    if CONVERT_DATA_TO_HUSTLE {
        use physical_operator::import_csv::ImportCsv;
        let import_operator = ImportCsv::new("data.csv".to_string(), relation.clone());
        import_operator.execute();

        use physical_operator::insert::Insert;
        use logical_operator::row::Row;
        let insert_operator = Insert::new(relation.clone(), Row::new(relation.get_schema().clone(),vec!(1,11)));
        //insert_operator.execute();
        //insert_operator.execute();
    }


    /* Read */

    {
        use physical_operator::select_sum::SelectSum;
        let select_operator = SelectSum::new(relation.clone(),Column::new("b".to_string(), 8));
        select_operator.execute();
    }

    {
        extern crate sqlite;
        let now = Instant::now();
        let connection = sqlite::open("sqlite.data").unwrap();
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
        use physical_operator::select_output::SelectOutput;
        let mut select_operator = SelectOutput::new(relation.clone());
        select_operator.execute();
        //select_operator.print();
    }

    {
        use physical_operator::join::Join;
        use physical_operator::select_sum::SelectSum;
        let join_operator = Join::new(relation.clone(), relation.clone());
        let joined_relation = join_operator.execute();
        let mut select_operator = SelectSum::new(joined_relation.clone(),Column::new("b".to_string(), 8));
        select_operator.execute();

        use physical_operator::select_output::SelectOutput;
        let mut select_operator = SelectOutput::new(joined_relation.clone());
        select_operator.execute();
       //select_operator.print();
    }

    {
        extern crate sqlite;
        let now = Instant::now();
        let connection = sqlite::open("sqlite.data").unwrap();
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

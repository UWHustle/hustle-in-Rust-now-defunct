extern crate csv;
extern crate rand;

use logical_entities::relation::Relation;
use logical_entities::value::Value;

use physical_operators::Operator;

use storage_manager::StorageManager;

#[derive(Debug)]
pub struct ExportCsv {
    file_name: String,
    relation: Relation,
    row_count: usize
}

impl ExportCsv {
    pub fn new(file_name: String, relation: Relation, row_count: usize) -> Self {
        ExportCsv {
            file_name,
            relation,
            row_count
        }
    }

    pub fn get_file_name(&self) -> &String {
        return &self.file_name;
    }
}

impl Operator for ExportCsv {
    fn execute(&self) -> Relation {
        let mut wtr = csv::Writer::from_path(self.get_file_name()).unwrap();

        let columns = self.relation.get_columns();
        let data = StorageManager::get_full_data(&self.relation.clone());

        let mut i = 0;
        while i < data.len() {

            let mut r = Vec::new();

            for column in columns {
                let value_length = column.get_datatype().get_next_length(&data[i..]);
                r.push(Value::format(column.get_datatype(),data[i..i+value_length].to_vec()));
                i += value_length;
            }
            wtr.write_record(&r).unwrap();

        }
        wtr.flush().unwrap();

        Relation::null()
    }
}
extern crate rand;
extern crate csv;

use logical_entities::relation::Relation;

#[derive(Debug)]
pub struct GenerateCsv {
    file_name: String,
    relation: Relation,
    row_count: usize
}

impl GenerateCsv {
    pub fn new(file_name: String, relation: Relation, row_count: usize) -> Self {
        GenerateCsv {
            file_name,
            relation,
            row_count
        }
    }

    pub fn get_file_name(&self) -> &String {
        return &self.file_name;
    }

    pub fn execute(&self) -> bool {
        let mut wtr = csv::Writer::from_path(self.get_file_name()).unwrap();

        let rows = self.row_count;
        let columns = self.relation.get_columns();

        for _y in 0..rows {
            let mut r = Vec::new();
            for _x in columns {
                let b: u8 = rand::random();
                r.push(b.to_string());
            }
            wtr.write_record(&r).unwrap();
        }
        wtr.flush().unwrap();
        true
    }
}
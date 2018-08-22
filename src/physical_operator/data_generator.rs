extern crate rand;
extern crate csv;

use logical_operator::relation::Relation;

#[derive(Debug)]
pub struct DataGenerator {
    file_name: String,
    relation: Relation,
    size: usize
}

impl DataGenerator {
    pub fn new(file_name: String, relation: Relation, size: usize) -> Self {
        DataGenerator {
            file_name,
            relation,
            size
        }
    }

    pub fn get_file_name(&self) -> &String {
        return &self.file_name;
    }

    pub fn execute(&self) -> bool {
        let mut wtr = csv::Writer::from_path(self.get_file_name()).unwrap();

        let rows = self.size;
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
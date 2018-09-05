extern crate csv;
extern crate rand;

use logical_entities::relation::Relation;
use physical_operators::select_output::SelectOutput;

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

    pub fn execute(&self) -> bool {
        let mut wtr = csv::Writer::from_path(self.get_file_name()).unwrap();

        let mut select_operator = SelectOutput::new(self.relation.clone());
        select_operator.execute();
        let rows = select_operator.get_result();

        for row in rows {
            let mut r = Vec::new();
            for value in row.get_values() {
                r.push(value.to_string());
            }
            wtr.write_record(&r).unwrap();
        }
        wtr.flush().unwrap();
        true
    }
}
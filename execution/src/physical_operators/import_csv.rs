use logical_entities::relation::Relation;
use physical_operators::Operator;
use type_system::*;

use super::storage::StorageManager;

extern crate csv;

pub struct ImportCsv {
    relation: Relation,
    filename: String,
}

impl ImportCsv {
    pub fn new(relation: Relation, filename: &str) -> Self {
        ImportCsv {
            relation,
            filename: String::from(filename),
        }
    }
}

impl Operator for ImportCsv {
    fn get_target_relation(&self) -> Relation {
        self.relation.clone()
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Relation, String> {
        let schema = self.relation.get_schema();
        storage_manager.delete(self.relation.get_name());

        let mut reader = match csv::Reader::from_path(&self.filename) {
            Ok(val) => val,
            Err(_err) => {
                return Err(String::from(format!(
                    "unable to open file '{}'",
                    self.filename
                )))
            }
        };
        reader.seek(csv::Position::new()).unwrap();

        for result in reader.records() {
            let csv_row = result.unwrap();
            let mut hustle_row = vec![];
            for col_i in 0..schema.get_columns().len() {
                let csv_str = csv_row.get(col_i).unwrap();
                let data_type = schema.get_columns()[col_i].data_type();
                let value = data_type.parse(csv_str).unwrap();
                hustle_row.extend_from_slice(value.un_marshall().data());
            }
            storage_manager.append(self.relation.get_name(), &hustle_row);
        }

        Ok(self.get_target_relation())
    }
}

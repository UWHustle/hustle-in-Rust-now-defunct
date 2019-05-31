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
    fn get_target_relation(&self) -> Option<Relation> {
        Some(self.relation.clone())
    }

    fn execute(&self, storage_manager: &StorageManager) -> Result<Option<Relation>, String> {
        let schema = self.relation.get_schema();
        storage_manager
            .relational_engine()
            .drop(self.relation.get_name());

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

        let physical_relation = storage_manager
            .relational_engine()
            .create(self.relation.get_name(), schema.to_size_vec());

        for result in reader.records() {
            let csv_row = result.unwrap();
            let mut row_builder = physical_relation.insert_row();
            for col_i in 0..schema.get_columns().len() {
                let csv_str = csv_row.get(col_i).unwrap();
                let data_type = schema.get_columns()[col_i].data_type();
                let value = data_type.parse(csv_str).unwrap();
                row_builder.push(value.un_marshall().data());
            }
        }

        Ok(self.get_target_relation())
    }
}

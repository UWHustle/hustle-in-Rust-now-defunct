use logical_entities::relation::Relation;
use physical_operators::Operator;
use logical_entities::types::*;

use storage_manager::StorageManager;

//#[derive(Debug)]
pub struct ImportCsv {
    file_name: String,
    relation: Relation
}

impl ImportCsv {
    pub fn new(file_name: String, relation: Relation) -> Self {
        ImportCsv {
            file_name,
            relation
        }
    }
}
impl Operator for ImportCsv{
    fn get_target_relation(&self) -> Relation {
        self.relation.clone()
    }

    fn execute(&self) -> Relation{

        extern crate csv;
        let mut rdr = csv::Reader::from_path(&self.file_name).unwrap();
        let record_count = rdr.records().count() + 1;
        rdr.seek(csv::Position::new()).unwrap();

        let mut data = StorageManager::create_relation(&self.relation, self.relation.get_row_size() * record_count);

        let columns = self.relation.get_columns();
        let mut n : usize = 0;

        for result in rdr.records() {
            let record = result.unwrap();

            for (i, column) in columns.iter().enumerate() {

                let a = record.get(i).unwrap().to_string();

                let c = column.get_datatype().parse(&a);
                let size = c.size();
                data[n..n + size].clone_from_slice(c.un_marshall().data()); // 0  8
                n = n + size;
            }
        }

        StorageManager::flush(&data);

        self.get_target_relation()
    }
}
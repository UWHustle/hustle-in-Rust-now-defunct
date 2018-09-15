use logical_entities::relation::Relation;
use logical_entities::row::Row;
use logical_entities::value::Value;

use storage_manager::StorageManager;

pub const CHUNK_SIZE:usize = 1024*1024;

#[derive(Debug)]
pub struct SelectOutput {
    relation: Relation,
    result: Vec<Row>,
}

impl SelectOutput {
    pub fn new(relation: Relation) -> SelectOutput {
        let result = vec!();
        SelectOutput {
            relation,
            result,
        }
    }

    pub fn get_result(&mut self) -> &mut Vec<Row> {
        return &mut self.result;
    }

    pub fn print(&mut self) -> bool {
        let width = 5;
        for column in self.relation.get_schema().get_columns() {
            print!("|{value:>width$}",value=column.get_name(), width=width);
        }
        println!("|");
        for row in self.get_result() {
            for value in row.get_values() {
                print!("|{value:>width$}",value=value, width=width);
            }
            println!("|");
        }
        true
    }


    pub fn execute(&mut self) -> bool{

        let columns = self.relation.get_columns();

        let data = StorageManager::get_data(self.relation.clone());

        let mut i = 0;
        while i < data.len() {
            let mut row_values:Vec<Value> = vec!();

            for column in columns {
                let value_length = column.get_datatype().get_next_length(&data[i..]);
                let value = Value::new(column.get_datatype(), data[i..i+value_length].to_vec());
                row_values.push(value);
                i += value_length;
            }

            let row = Row::new(self.relation.get_schema().clone(), row_values);
            self.result.push(row);
        }

        true
    }
}
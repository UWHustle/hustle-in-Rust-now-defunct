use entities::column::Column;
use entities::block::Block;
use entities::datatype::DataType;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Relation {
    pub name: String,
    pub columns: Vec<Column>,
    pub blocks: Vec<Block>,
}

impl Relation {
    pub fn new(name: String) -> Relation {
        Relation {
            name: name,
            columns: vec!(),
            blocks: vec!()
        }
    }

    pub fn add_column(&mut self, name: String) -> () {
        let data_type = DataType::new("Default".to_string());
        self.columns.push(Column::new(name, data_type));
    }


    pub fn get_columns(&mut self) -> Vec<Column> {
        self.columns.to_owned()
    }

}
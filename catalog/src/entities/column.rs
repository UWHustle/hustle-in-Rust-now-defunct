use entities::datatype::DataType;
use entities::statistic::Statistic;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub stats: Vec<Statistic>,
}

impl Column {
    pub fn new(name: String, datatype: DataType) -> Column {
        Column {
            name: name,
            datatype: datatype,
            stats: vec!()
        }
    }
}
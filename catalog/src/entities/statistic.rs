#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Statistic {
    key: String,
    value: u8,
}
use logical_entities::types::DataType;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Column {
    name: String,
    size: usize,
    datatype: DataType,
}

impl Column {
    pub fn new(name: String, thistype: String) -> Self {
        let datatype;
        if thistype == "Int" || thistype == "Int NULL"
        {
            datatype = DataType::Integer;
        }
        else {datatype = DataType::IpAddress;}
        let size: usize = DataType::get_next_length(&datatype, Vec::new().as_slice());
        Column {
            name, size, datatype
        }
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }
    pub fn get_size(&self) -> usize {
        return self.size;
    }
    pub fn get_datatype(&self) -> DataType { return self.datatype.clone();}
}

#[cfg(test)]
mod tests {
    #[test]
    fn column_create() {
        use logical_entities::column::Column;
        let column = Column::new("test".to_string(),"Int".to_string());
        assert_eq!(column.get_name(),&"test".to_string());
assert_eq!(column.get_size(), 8);
    }

}
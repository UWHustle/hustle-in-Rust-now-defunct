use logical_entities::types::DataType;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Column {
    name: String,
    size: usize,
    data_type: DataType,
}

impl Column {
    pub fn new(name: String, type_string: String) -> Self {
        let data_type;
        if type_string == "Int" || type_string == "Int NULL" {
            data_type = DataType::Integer;
        } else {
            data_type = DataType::IpAddress;
        }
        let size: usize = DataType::get_next_length(&data_type, Vec::new().as_slice());
        Column {
            name,
            size,
            data_type,
        }
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn get_size(&self) -> usize {
        return self.size;
    }

    pub fn get_datatype(&self) -> DataType {
        return self.data_type.clone();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn column_create() {
        use logical_entities::column::Column;
        let column = Column::new("test".to_string(), "Int".to_string());
        assert_eq!(column.get_name(), &"test".to_string());
        assert_eq!(column.get_size(), 8);
    }
}
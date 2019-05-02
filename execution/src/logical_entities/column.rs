use type_system::data_type::*;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Column {
    name: String,
    data_type: DataType,
}

impl Column {
    pub fn new(name: &str, data_type: DataType) -> Self {
        Column {
            name: String::from(name),
            data_type,
        }
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_size(&self) -> usize {
        self.data_type.size()
    }

    pub fn data_type(&self) -> DataType {
        self.data_type.clone()
    }
}

#[cfg(test)]
mod tests {
    use logical_entities::column::Column;
    use type_system::data_type::*;

    #[test]
    fn column_create() {
        let data_type = DataType::new(Variant::Int4, true);
        let column = Column::new("test", data_type.clone());
        assert_eq!(column.get_name(), "test");
        assert_eq!(column.get_size(), data_type.size());
    }
}

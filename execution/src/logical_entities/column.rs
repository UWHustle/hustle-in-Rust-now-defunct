use logical_entities::types::TypeID;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Column {
    name: String,
    size: usize,
    data_type: TypeID,
}

impl Column {
    pub fn new(name: String, type_string: String) -> Self {
        let data_type = TypeID::from_string(&type_string);
        let size: usize = data_type.size();
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

    pub fn get_datatype(&self) -> TypeID {
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
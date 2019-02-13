use type_system::type_id::*;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Column {
    name: String,
    data_type: TypeID,
}

impl Column {
    pub fn new(name: String, type_id: TypeID) -> Self {
        Column {
            name,
            data_type: type_id,
        }
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn get_size(&self) -> usize {
        return self.data_type.size();
    }

    pub fn get_datatype(&self) -> TypeID {
        self.data_type.clone()
    }
}

#[cfg(test)]
mod tests {
    use logical_entities::column::Column;
    use type_system::type_id::*;

    #[test]
    fn column_create() {
        let type_id = TypeID::new(Variant::Int4, true);
        let column = Column::new("test".to_string(), type_id.clone());
        assert_eq!(column.get_name(), &"test".to_string());
        assert_eq!(column.get_size(), type_id.size());
    }
}
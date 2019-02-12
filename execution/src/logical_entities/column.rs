use type_system::*;
use type_system::type_id::TypeID;

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
        return self.data_type.clone();
    }
}

#[cfg(test)]
mod tests {
    use type_system::type_id::*;

    #[test]
    fn column_create() {
        use logical_entities::column::Column;
        let column = Column::new("test".to_string(), TypeID::Int4(true));
        assert_eq!(column.get_name(), &"test".to_string());
        assert_eq!(column.get_size(), 8);
    }
}
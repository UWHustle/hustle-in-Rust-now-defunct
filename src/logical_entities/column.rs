use logical_entities::types::DataType;

#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    name: String,
    size: usize,
    datatype: DataType,
}

impl Column {
    pub fn new(name: String, size: usize) -> Self {
        use logical_entities::types::integer::IntegerType;
        let datatype = DataType::Integer;
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


use std::os::raw::c_char;
use std::ffi::CStr;

#[repr(C)]
#[derive(Clone, Debug, PartialEq)]
pub struct ExtColumn {
    name: *const c_char,
}

impl ExtColumn {
    pub fn to_column(&self) -> Column {
        let c_name = self.name;
        assert!(!c_name.is_null());


        let c_str = unsafe { CStr::from_ptr(c_name) };
        let name = c_str.to_str().expect("Column name not a valid UTF-8 string").to_string();
        let size = 8;

        Column::new(name,size)
    }

    pub fn from_column(column:Column)-> (ExtColumn) {
        let r_name = column.get_name();
        let mut c_name;

        unsafe {
            c_name = r_name.as_ptr() as *const c_char;
        }

        ExtColumn {
            name: c_name
        }
    }

    pub fn is_valid(&self) -> bool {
        !self.name.is_null()
    }
}


#[cfg(test)]
mod tests {
    #[test]
    fn column_create() {
        use logical_entities::column::Column;
        let column = Column::new("test".to_string(),8);
        assert_eq!(column.get_name(),&"test".to_string());
        assert_eq!(column.get_size(), 8);
    }

    #[test]
    fn ext_column_create() {
        use logical_entities::column::ExtColumn;
        use logical_entities::column::Column;
        let column1 = Column::new("test1".to_string(),8);
        let column2 = Column::new("test2".to_string(),8);

        use std::ffi::CStr;
        let ext_column1 = ExtColumn::from_column(column1);
        unsafe {
            assert_eq!(CStr::from_ptr(ext_column1.name).to_str().unwrap(), "test1");
        }

        let ext_column2 = ExtColumn::from_column(column2);
        unsafe {
            assert_eq!(CStr::from_ptr(ext_column2.name).to_str().unwrap(), "test2");
        }

        let r_column = ext_column1.to_column();
        assert_eq!(r_column.get_name(),&"test1".to_string());
        assert_eq!(r_column.get_size(), 8);
    }
}
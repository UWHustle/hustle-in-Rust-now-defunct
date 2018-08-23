
#[derive(Clone, Debug)]
pub struct Column {
    name: String,
    size: usize,
}

impl Column {
    pub fn new(name: String, size: usize) -> Self {
        Column {
            name, size
        }
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }
    pub fn get_size(&self) -> usize {
        return self.size;
    }
}


use std::os::raw::c_char;
use std::ffi::CStr;

#[repr(C)]
#[derive(Copy, Clone)]
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

        Column {
            name, size
        }
    }

    pub fn from_column(column:Column)-> ExtColumn {
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
    fn c_column_create() {
        use logical_entities::column::ExtColumn;
        use logical_entities::column::Column;
        let column = Column::new("test".to_string(),8);

        use std::os::raw::c_char;
        use std::ffi::CStr;
        let c_column = ExtColumn::from_column(column);
        unsafe {
            assert_eq!(CStr::from_ptr(c_column.name).to_str().unwrap(), "test");
        }

        let r_column = c_column.to_column();
        assert_eq!(r_column.get_name(),&"test".to_string());
        assert_eq!(r_column.get_size(), 8);
    }
}
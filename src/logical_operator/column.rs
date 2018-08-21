
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

#[repr(C)]
#[derive(Copy, Clone)]
pub struct cColumn {
    name: *const c_char,
}

impl cColumn {
    pub fn to_column(&self) -> Column {
        let c_name = self.name;
        assert!(!c_name.is_null());

        use std::ffi::CStr;
        let c_str = unsafe { CStr::from_ptr(c_name) };
        let name = c_str.to_str().expect("Column name not a valid UTF-8 string").to_string();
        let size = 8;

        Column {
            name, size
        }
    }

    pub fn is_valid(&self) -> bool {
        !self.name.is_null()
    }
}
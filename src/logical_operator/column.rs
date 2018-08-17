
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
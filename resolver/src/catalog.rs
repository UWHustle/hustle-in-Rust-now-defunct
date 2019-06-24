use std::collections::HashSet;
use message::Table;
use std::fs::File;
use std::fs;

const CATALOG_FILE_NAME: &str = "catalog.json";

#[derive(Serialize, Deserialize)]
pub struct Catalog {
    tables: HashSet<Table>
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            tables: HashSet::new()
        }
    }

    pub fn try_from_file() -> Option<Self> {
        let file = File::open(CATALOG_FILE_NAME).ok()?;
        serde_json::from_reader(file).ok()
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains(name)
    }

    pub fn create_table(&mut self, table: Table) -> Result<(), String> {
        self.tables.insert(table);
        self.flush()
    }

    pub fn drop_table(&mut self, name: &str) -> Result<(), String> {
        self.tables.remove(name);
        self.flush()
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    fn flush(&self) -> Result<(), String> {
        let buf = serde_json::to_vec(self)
            .map_err(|e| e.to_string())?;
        fs::write(CATALOG_FILE_NAME, buf)
            .map_err(|e| e.to_string())
    }
}

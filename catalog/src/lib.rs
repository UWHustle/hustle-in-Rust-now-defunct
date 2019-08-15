#[macro_use]
extern crate serde;

use std::collections::HashSet;

use std::fs;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::borrow::Borrow;
use std::sync::RwLock;
use hustle_types::DataType;

const CATALOG_FILE_NAME: &str = "catalog.json";

#[derive(Serialize, Deserialize)]
pub struct Catalog {
    tables: RwLock<HashSet<Table>>,
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            tables: RwLock::new(HashSet::new()),
        }
    }

    pub fn try_from_file() -> Option<Self> {
        let file = File::open(CATALOG_FILE_NAME).ok()?;
        serde_json::from_reader(file).ok()
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.read().unwrap().contains(name)
    }

    pub fn create_table(&self, table: Table) -> Result<(), String> {
        let mut tables = self.tables.write().unwrap();
        tables.insert(table);
        self.flush()
    }

    pub fn drop_table(&self, name: &str) -> Result<(), String> {
        let mut tables = self.tables.write().unwrap();
        tables.remove(name);
        self.flush()
    }

    fn flush(&self) -> Result<(), String> {
        let buf = serde_json::to_vec(self)
            .map_err(|e| e.to_string())?;
        fs::write(CATALOG_FILE_NAME, buf)
            .map_err(|e| e.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub block_ids: Vec<u64>,
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>, block_ids: Vec<u64>) -> Self {
        Table {
            name,
            columns,
            block_ids,
        }
    }
}

impl Hash for Table {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        self.name.eq(&other.name)
    }
}

impl Eq for Table {}

impl Borrow<str> for Table {
    fn borrow(&self) -> &str {
        self.name.borrow()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: HustleType,
}
